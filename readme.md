# Multi Version B+Tree
- Release Date: 06.12.2024
- Latest Version: 0.0.57 (Beta)
---------------------------------------
## PiBench Integration: https://github.com/umr-dbs/pibench_ext
# Concurrency Controls
- **MonoWriter**: 1x writer, unlimited readers.
- **OLC**: Unlimited all.
- **ORWC**: Deprecated; Optimistic Upgrade not working.
# Transactions support:
  - Transactions via Si.
  - (CRUD) AtomicTransactions via Si.
# Built-in (On/Off) GC via TransactionsManager.
---------------------------------------
# Contact
    Name:               Amir El-Shaikh
    E-Mail:             elshaikh@mathematik.uni-marburg.de