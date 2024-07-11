
set -eu

EXE=$(cargo build --release --bin "$2" \
        --message-format=json \
        | jq -r 'select(.reason == "compiler-artifact") | select(.executable != null) | .executable')
cp "$EXE" "$3"
redo-always
redo-stamp <"$3"

