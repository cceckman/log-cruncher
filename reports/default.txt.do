
set -eu
redo-ifchange ../quarantine/gcs.db joins.sql "$2".sql

sqlite3 -header -column <"$2".sql >"$3" ../quarantine/gcs.db

