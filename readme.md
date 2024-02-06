# Multi Version B+Tree (MV-BPlusTree) Repository
- Build: 06.01.2024
- Version: 0.0.17 (Beta)
---------------------------------------
# Supported Operations
- Insert (Key, Payload)
- Point Query (Key, Version)
- **Snapshot** Point Query (Key)
- Range Query ([key_min, key_max], Version)
- **Snapshot** Range Query ([key_min, key_max])
- _Lazy Iterator_ Range Query ([key_min, key_max], Version)
- **Snapshot** _Lazy Iterator_ Range Query ([key_min, key_max])
- **Snapshot** Dispatch-Loop Transaction
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