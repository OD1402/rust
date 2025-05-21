cd ~/abc/src/rust/rysev_back
RUST_LOG=info cargo run --release -- for-analytics 2>&1 | tee SomeFile.txt
