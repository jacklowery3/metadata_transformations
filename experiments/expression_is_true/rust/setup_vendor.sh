#!/usr/bin/env bash
# Builds vendor/iceberg-rust: apache/iceberg-rust with the few crate-private items
# this experiment needs made public (metrics evaluators, manifest cache, DataFile
# fields). Results in RESULTS.md were measured at ICEBERG_RUST_REF below.
#   ICEBERG_RUST_SRC=/path/to/clone ./setup_vendor.sh   # use an existing clone as-is
#   ./setup_vendor.sh                                   # fetch the pinned commit
set -euo pipefail
ICEBERG_RUST_REF="${ICEBERG_RUST_REF:-ab047b5}"
HERE="$(cd "$(dirname "$0")" && pwd)"
DST="$HERE/vendor/iceberg-rust"
if [ -n "${ICEBERG_RUST_SRC:-}" ]; then
  SRC="$ICEBERG_RUST_SRC"
else
  SRC="$HERE/vendor/src"
  if [ ! -d "$SRC/.git" ]; then
    git clone --quiet --filter=blob:none https://github.com/apache/iceberg-rust.git "$SRC"
  fi
  git -C "$SRC" checkout --quiet "$ICEBERG_RUST_REF"
fi
mkdir -p "$DST"
rsync -a --delete --exclude .git --exclude target "$SRC/" "$DST/"
I="$DST/crates/iceberg/src"
for f in expr/mod.rs expr/visitors/mod.rs \
         expr/visitors/inclusive_metrics_evaluator.rs expr/visitors/strict_metrics_evaluator.rs \
         io/mod.rs io/object_cache.rs table.rs spec/manifest/data_file.rs; do
  sed -i.bak 's/pub(crate)/pub/g' "$I/$f" && rm -f "$I/$f.bak"
done
sed -i.bak '/#!\[deny(missing_docs)\]/d' "$I/lib.rs" && rm -f "$I/lib.rs.bak"
echo "vendored iceberg-rust $(git -C "$SRC" log -1 --format='%h %cs') -> $DST"
