#!/usr/bin/env bash
# ------------------------------------------------------------
# init_kb.sh  —  bootstrap modops_trader knowledge-base layout
# Usage: ./init_kb.sh <KB_ROOT_ON_D> <VECTOR_STORE_SRC>
# ------------------------------------------------------------
set -euo pipefail

KB_ROOT="${1:-/mnt/d/modops_kb}"          # Posix view of D:
SRC_VSTORE="${2:-}"

echo "📂  Creating knowledge-base skeleton at ${KB_ROOT}"
mkdir -p "${KB_ROOT}"/{raw_data,documents,processed,embeddings,graph_db,\
ingestion_scripts,configs,vector_store,logs,tmp}

# --- move or (re)create vector_store.py ---------------------
DST_VSTORE="${KB_ROOT}/vector_store/vector_store.py"
if [[ -n "${SRC_VSTORE}" && -f "${SRC_VSTORE}" ]]; then
  echo "🚚  Moving existing vector_store.py → ${DST_VSTORE}"
  mv "${SRC_VSTORE}" "${DST_VSTORE}"
else
  echo "🆕  No source vector_store supplied — creating empty stub"
  cat <<'PY' > "${DST_VSTORE}"
\"\"\"Vector store stub — implement FAISS/Chroma connector here.\"\"\"
PY
fi

# --- touch empty config & ETL placeholders -------------------
touch "${KB_ROOT}/configs/"{vector_db.yaml,graph_db.yaml,ingestion.yaml}
touch "${KB_ROOT}/ingestion_scripts/"{__init__.py,ingest_ticks.py,ingest_options.py}

echo "✅  Done — folder tree:"
tree -L 2 "${KB_ROOT}" || find "${KB_ROOT}" -maxdepth 2 -print
