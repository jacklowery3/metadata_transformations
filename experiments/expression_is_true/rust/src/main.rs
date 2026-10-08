//! expression_is_true decided from Iceberg metadata, in Rust (iceberg-rust).
//!
//! Same rules as mdt/decide.py (decide_expr):
//!   inclusive(NOT expr)  false -> the file can't hold a violation
//!   strict(NOT expr)     true  -> every row in the file violates
//!   witness              an exact bound (or a NULL) violates -> a violation exists;
//!                        tested by asking the strict evaluator about a fake
//!                        one-row file holding just that value
//!   single-valued file   exact min == max -> decided by that value
//!   else                 read only the undecided files
//!
//! Cases come from ../cases.json (+ gitignored ../cases.local.json). The small
//! expression grammar below covers them; anything else (e.g. column vs column)
//! is UNKNOWN. Output: one JSON line per (case, rep) with per-phase timings.
//!
//!     ./setup_vendor.sh && cargo run --release -- [reps] > results.jsonl

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use futures::{StreamExt, TryStreamExt, stream};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::expr::visitors::inclusive_metrics_evaluator::InclusiveMetricsEvaluator;
use iceberg::expr::visitors::strict_metrics_evaluator::StrictMetricsEvaluator;
use iceberg::expr::{Bind, BoundPredicate, Predicate, Reference};
use iceberg::spec::{
    DataContentType, DataFile, Datum, ManifestContentType, PrimitiveLiteral, PrimitiveType, Schema, Type,
};
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_storage_opendal::OpenDalStorageFactory;
use regex::Regex;
use serde_json::{Value, json};

// ---------------------------------------------------------------- expression grammar

/// Parse `col <op> lit`, `col between a and b`, `col in (...)`, `col is [not] null`.
/// Returns (predicate, column, tests_null) or None if not expressible.
fn parse_expr(s: &str, schema: &Schema) -> Option<(Predicate, String, bool)> {
    let s = s.trim();
    let null_re = Regex::new(r"(?i)^(\w+)\s+is\s+(not\s+)?null$").unwrap();
    let between_re = Regex::new(r"(?i)^(\w+)\s+between\s+(.+?)\s+and\s+(.+)$").unwrap();
    let in_re = Regex::new(r"(?i)^(\w+)\s+in\s*\((.*)\)$").unwrap();
    let cmp_re = Regex::new(r"^(\w+)\s*(>=|<=|!=|<>|=|>|<)\s*(.+)$").unwrap();

    if let Some(c) = null_re.captures(s) {
        let col = c[1].to_string();
        schema.field_by_name(&col)?;
        let p = if c.get(2).is_some() { Reference::new(&col).is_not_null() } else { Reference::new(&col).is_null() };
        return Some((p, col, true));
    }
    if let Some(c) = between_re.captures(s) {
        let col = c[1].to_string();
        let (lo, hi) = (lit(&c[2], &col, schema)?, lit(&c[3], &col, schema)?);
        let p = Reference::new(&col).greater_than_or_equal_to(lo).and(Reference::new(&col).less_than_or_equal_to(hi));
        return Some((p, col, false));
    }
    if let Some(c) = in_re.captures(s) {
        let col = c[1].to_string();
        let vals: Option<Vec<Datum>> = c[2].split(',').map(|v| lit(v, &col, schema)).collect();
        return Some((Reference::new(&col).is_in(vals?), col, false));
    }
    if let Some(c) = cmp_re.captures(s) {
        let col = c[1].to_string();
        let v = lit(&c[3], &col, schema)?; // None if the RHS is a column, not a literal
        let r = Reference::new(&col);
        let p = match &c[2] {
            ">=" => r.greater_than_or_equal_to(v),
            "<=" => r.less_than_or_equal_to(v),
            ">" => r.greater_than(v),
            "<" => r.less_than(v),
            "=" => r.equal_to(v),
            _ => r.not_equal_to(v),
        };
        return Some((p, col, false));
    }
    None
}

/// A SQL literal typed by the column's Iceberg type.
fn lit(raw: &str, col: &str, schema: &Schema) -> Option<Datum> {
    let raw = raw.trim();
    let quoted = raw.len() >= 2 && raw.starts_with('\'') && raw.ends_with('\'');
    let text = if quoted { &raw[1..raw.len() - 1] } else { raw };
    let Type::Primitive(t) = schema.field_by_name(col)?.field_type.as_ref() else { return None };
    match t {
        PrimitiveType::Int => text.parse::<i32>().ok().map(Datum::int),
        PrimitiveType::Long => text.parse::<i64>().ok().map(Datum::long),
        PrimitiveType::Double => text.parse::<f64>().ok().map(Datum::double),
        PrimitiveType::Boolean => text.parse::<bool>().ok().map(Datum::bool),
        PrimitiveType::String if quoted => Some(Datum::string(text)),
        PrimitiveType::Timestamptz if quoted => Datum::timestamptz_from_str(text).ok(),
        PrimitiveType::Date if quoted => Datum::date_from_str(text).ok(),
        _ => None,
    }
}

// ---------------------------------------------------------------- decide

/// Inferred string-bound truncation: the longest string bound seen (>= Iceberg's 16).
fn infer_trunc(files: &[DataFile], schema: &Schema) -> usize {
    let strs: HashSet<i32> = schema
        .as_struct()
        .fields()
        .iter()
        .filter(|f| matches!(f.field_type.as_ref(), Type::Primitive(PrimitiveType::String)))
        .map(|f| f.id)
        .collect();
    let mut longest = 16;
    for f in files {
        for (fid, d) in f.lower_bounds.iter().chain(f.upper_bounds.iter()) {
            if strs.contains(fid) {
                if let PrimitiveLiteral::String(s) = d.literal() {
                    longest = longest.max(s.chars().count());
                }
            }
        }
    }
    longest
}

/// The bound as a value guaranteed to occur in the file, if it is one.
fn exact(d: Option<&Datum>, trunc: usize) -> Option<Datum> {
    let d = d?;
    match d.literal() {
        PrimitiveLiteral::String(s) if s.chars().count() >= trunc => None,
        PrimitiveLiteral::Binary(_) | PrimitiveLiteral::Float(_) | PrimitiveLiteral::Double(_) => None,
        _ => Some(d.clone()),
    }
}

/// Would a row whose `fid` holds `v` (None = NULL) violate? Asks the strict evaluator
/// about a one-row file with just that value.
fn violates(bound: &BoundPredicate, template: &DataFile, fid: i32, v: Option<&Datum>) -> iceberg::Result<bool> {
    let mut f = template.clone();
    f.record_count = 1;
    f.value_counts = HashMap::from([(fid, 1)]);
    f.null_value_counts = HashMap::from([(fid, if v.is_none() { 1 } else { 0 })]);
    // iceberg-rust's strict evaluator treats a missing NaN count as "may contain NaN"
    // for every type, so state it explicitly (exact bounds exclude NaN).
    f.nan_value_counts = HashMap::from([(fid, 0)]);
    f.lower_bounds = v.map(|v| HashMap::from([(fid, v.clone())])).unwrap_or_default();
    f.upper_bounds = f.lower_bounds.clone();
    StrictMetricsEvaluator::eval(bound, &f)
}

async fn data_files(table: &Table) -> iceberg::Result<Vec<DataFile>> {
    let Some(snapshot) = table.metadata().current_snapshot() else { return Ok(vec![]) };
    let cache = table.object_cache();
    let list = cache.get_manifest_list(snapshot, &table.metadata_ref()).await?;
    let manifests: Vec<_> = stream::iter(list.entries().iter().filter(|m| m.content == ManifestContentType::Data))
        .map(|m| cache.get_manifest(m))
        .buffer_unordered(64)
        .try_collect()
        .await?;
    Ok(manifests
        .iter()
        .flat_map(|m| m.entries().iter())
        .filter(|e| e.is_alive() && e.data_file().content_type() == DataContentType::Data)
        .map(|e| e.data_file().clone())
        .collect())
}

async fn decide(table: &Table, expression: &str) -> iceberg::Result<Value> {
    let schema = table.metadata().current_schema().clone();
    let Some((expr, col, null_test)) = parse_expr(expression, &schema) else {
        return Ok(json!({"verdict": "UNKNOWN", "how": "not a single-column Iceberg predicate"}));
    };
    let mut neg = !expr;
    if !null_test {
        neg = neg.and(Reference::new(&col).is_not_null()); // SQL: a NULL comparison isn't a violation
    }
    let neg = neg.rewrite_not();
    let bound = neg.bind(schema.clone(), true)?;
    let fid = schema.field_by_name(&col).unwrap().id;

    let t = Instant::now();
    let files = data_files(table).await?;
    let trunc = infer_trunc(&files, &schema);
    let manifests_ms = t.elapsed().as_secs_f64() * 1e3;

    let t = Instant::now();
    let (mut might, mut must, mut witness, mut settled) = (0, 0u64, 0, 0);
    let mut undecided: HashSet<String> = HashSet::new();
    for f in &files {
        if !InclusiveMetricsEvaluator::eval(&bound, f, false)? {
            continue;
        }
        might += 1;
        if f.record_count > 0 && StrictMetricsEvaluator::eval(&bound, f)? {
            must += f.record_count;
            continue;
        }
        let lo = exact(f.lower_bounds.get(&fid), trunc);
        let hi = exact(f.upper_bounds.get(&fid), trunc);
        let nulls = f.null_value_counts.get(&fid).copied();
        let mut wit = false;
        for v in [&lo, &hi].into_iter().flatten() {
            wit |= violates(&bound, f, fid, Some(v))?;
        }
        if nulls.unwrap_or(0) > 0 {
            wit |= violates(&bound, f, fid, None)?;
        }
        if wit {
            witness += 1;
        } else if lo.is_some() && lo == hi && (nulls == Some(0) || !null_test) {
            settled += 1; // single-valued and OK; NULLs can't violate a null-rejecting check
        } else {
            undecided.insert(f.file_path.clone());
        }
    }
    let eval_ms = t.elapsed().as_secs_f64() * 1e3;

    let mut out = json!({"files": files.len(), "might": might, "must_rows": must, "witness": witness,
                         "single_value_ok": settled, "scanned_files": 0,
                         "manifests_ms": manifests_ms, "eval_ms": eval_ms, "scan_ms": 0.0});
    if must > 0 || witness > 0 {
        out["verdict"] = json!("FAIL");
        out["how"] = json!("metadata");
    } else if undecided.is_empty() {
        out["verdict"] = json!("PASS");
        out["how"] = json!("metadata");
    } else {
        let t = Instant::now();
        let scan = table.scan().select([col.as_str()]).with_filter(neg.clone()).build()?;
        let tasks: Vec<_> = scan
            .plan_files()
            .await?
            .try_filter(|task| futures::future::ready(undecided.contains(task.data_file_path())))
            .try_collect()
            .await?;
        let scanned = tasks.len();
        let reader = ArrowReaderBuilder::new(table.file_io().clone(), table.runtime().clone()).build();
        let batches: Vec<_> = reader.read(stream::iter(tasks.into_iter().map(Ok)).boxed())?.stream().try_collect().await?;
        let n: usize = batches.iter().map(|b| b.num_rows()).sum();
        out["scan_ms"] = json!(t.elapsed().as_secs_f64() * 1e3);
        out["scanned_files"] = json!(scanned);
        out["failures"] = json!(n);
        out["verdict"] = json!(if n > 0 { "FAIL" } else { "PASS" });
        out["how"] = json!("narrowed scan");
    }
    Ok(out)
}

// ---------------------------------------------------------------- config & main

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..").canonicalize().unwrap()
}

/// POLARIS_* env vars, else <repo>/.env, else the lakehouse-explorer skill config
/// (same resolution as mdt/catalog.py).
fn polaris_props() -> HashMap<String, String> {
    let mut env: HashMap<String, String> = std::env::vars().collect();
    if !env.contains_key("POLARIS_CATALOG_URI") {
        if let Ok(text) = std::fs::read_to_string(repo_root().join(".env")) {
            for line in text.lines().map(str::trim).filter(|l| !l.starts_with('#')) {
                if let Some((k, v)) = line.split_once('=') {
                    env.entry(k.trim().into()).or_insert_with(|| v.trim().trim_matches('"').into());
                }
            }
        }
    }
    if !env.get("POLARIS_CATALOG_URI").is_some_and(|v| !v.is_empty()) {
        let path = format!("{}/.claude/skills/lakehouse-explorer/.lakehouse_config.json", env["HOME"]);
        let cfg: Value = serde_json::from_str(&std::fs::read_to_string(path).expect("no Polaris config")).unwrap();
        let p = &cfg["polaris"];
        let s = |k: &str| p[k].as_str().unwrap().to_string();
        env.insert("POLARIS_CATALOG_URI".into(), s("polaris_endpoint"));
        env.insert("POLARIS_WAREHOUSE".into(), s("polaris_warehouse"));
        env.insert("POLARIS_CLIENT_ID".into(), s("polaris_client_id"));
        env.insert("POLARIS_CLIENT_SECRET".into(), s("polaris_client_secret"));
        env.insert("BUCKET_REGION".into(), s("bucket_region"));
    }
    let uri = env["POLARIS_CATALOG_URI"].clone();
    let token = env.get("POLARIS_TOKEN_URI").filter(|v| !v.is_empty()).cloned()
        .unwrap_or_else(|| format!("{uri}/v1/oauth/tokens"));
    let mut props = HashMap::from([
        ("uri".into(), uri),
        ("warehouse".into(), env["POLARIS_WAREHOUSE"].clone()),
        ("credential".into(), format!("{}:{}", env["POLARIS_CLIENT_ID"], env["POLARIS_CLIENT_SECRET"])),
        ("scope".into(), env.get("POLARIS_SCOPE").cloned().unwrap_or_else(|| "PRINCIPAL_ROLE:ALL".into())),
        ("oauth2-server-uri".into(), token),
        ("header.X-Iceberg-Access-Delegation".into(), "vended-credentials".into()),
    ]);
    if let Some(r) = env.get("BUCKET_REGION").filter(|v| !v.is_empty()) {
        props.insert("s3.region".into(), r.clone());
    }
    props
}

fn load_cases() -> Vec<Value> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    ["cases.json", "cases.local.json"]
        .iter()
        .filter_map(|n| std::fs::read_to_string(dir.join(n)).ok())
        .flat_map(|t| serde_json::from_str::<Vec<Value>>(&t).unwrap())
        .collect()
}

#[tokio::main]
async fn main() -> iceberg::Result<()> {
    let reps: usize = std::env::args().nth(1).and_then(|a| a.parse().ok()).unwrap_or(3);
    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 { customized_credential_load: None }))
        .load("polaris", polaris_props())
        .await?;
    for case in load_cases() {
        let table_name = case["table"].as_str().unwrap();
        let expression = case["expression"].as_str().unwrap();
        let label = case["alias"].as_str().map(String::from).unwrap_or(format!("{table_name}: {expression}"));
        for rep in 0..reps {
            let t = Instant::now();
            let table = catalog.load_table(&TableIdent::from_strs(table_name.split('.'))?).await?;
            let load_ms = t.elapsed().as_secs_f64() * 1e3;
            let mut out = decide(&table, expression).await?;
            out["engine"] = json!("iceberg-rust");
            out["case"] = json!(label);
            out["rep"] = json!(rep);
            out["load_table_ms"] = json!(load_ms);
            out["total_ms"] = json!(t.elapsed().as_secs_f64() * 1e3);
            println!("{out}");
        }
    }
    Ok(())
}
