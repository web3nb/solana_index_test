rm -rf ./target && rm -rf ./src/pb
substreams protogen ./substreams.yaml --exclude-paths='sf/substreams,google'
cargo build --target wasm32-unknown-unknown --release
substreams pack ./substreams.yaml
graph codegen
graph build
