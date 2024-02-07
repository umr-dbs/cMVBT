# Multi Version B+Tree (MV-BPlusTree) Repository
- Build: 07.01.2024
- Version: 0.0.18 (Beta)
---------------------------------------
# Concurrency Controls
- **MonoWriter**:
  - One writer.
  - Unlimited lock-free readers.
- **ORWC**, **OLC**, **LHL** and **HL**:
  - Unlimited writers.
  - Unlimited lock-free readers.
# Operations (CC Built-in)
- ### CreateReadUpdateDelete (CRUD) 
  - Insert (Key, Payload)
  - Delete (Key)
  - Update (Key, Payload)
  - Point Query (Key, Version)
  - Range Query ([key_min, key_max], Version)
  - _Lazy Iterator_ Range Query ([key_min, key_max], Version)
- ### Snapshot Isolation
  - Atomic Transaction: A single CRUD Operation on a Snapshot.
  - Transaction: Collection of CRUD on a Snapshot.
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