
set -eu
# We don't auto-rerun on DB update; want to manually poke anything that reaches off-machine.
redo-ifchange joins.sql "$2".sql

sqlite3 -header -column <"$2".sql >"$3" ../quarantine/gcs.db

