# Multi Version B+Tree (MV-BPlusTree) Repository
- Build:             - 01.01.2023
- Version:           - 0.0.14 (Beta)
---------------------------------------
# Supported Operations
- Insert (Key, Payload)
- Point Query (Key, Version)
- Range Query ([key_min, key_max], Version)
- Snapshot (Version)
- Dispatch-Loop Transaction
---------------------------------------
# Build
#### Requirements:
- OS:          Linux
- Rustc:       >= 1.60.0 (2021 Edition)
    
#### Build:
- Standalone as `cargo build --release`.
---------------------------------------
# Contact
    Name:               Amir El-Shaikh
    E-Mail:             elshaikh@mathematik.uni-marburg.de