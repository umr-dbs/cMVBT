# Multi Version B+Tree
- Release Date: 04.07.2024
- Latest Version: 0.0.42 (Beta)
#### Requirements:
- OS:          Linux/Windows
- Rustc:       >= 1.65.0 (2021 Edition)

#### Build:
- Standalone as `cargo build --release`.
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
# Transaction Manager with Basic GC
  - Execute Transactions
  - Execute AtomicTransactions
  - Execute Non-reader Transactions
  - Execute Non-reader AtomicTransactions
---------------------------------------
# Contact
    Name:               Amir El-Shaikh
    E-Mail:             elshaikh@mathematik.uni-marburg.de