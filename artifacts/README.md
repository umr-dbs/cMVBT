# Prebuilt cMVBT binary

`bin/cMVBT-v0.0.110-linux-x86_64-glibc238` is a convenience executable for
artifact inspection and smoke tests. It is not the reference binary for
performance measurements.

## Compatibility

- operating system: x86-64 Linux;
- minimum detected glibc symbol version: 2.38;
- dynamic libraries: glibc, libm, and libgcc_s;
- cMVBT version: 0.0.110.

The executable was built from Git revision
`179dc8afe0a3d2269b7581c37eb44028a02cf6d4` with Rust 1.99.0 using:

```bash
cargo build --profile paper
```

The binary was intentionally built without `target-cpu=native` so it can run on
a wider range of x86-64 CPUs. The repository now applies that setting correctly
to new source builds; use a fresh source build on the evaluation machine for
performance measurements.

## Verify and run

From the repository root:

```bash
sha256sum --check artifacts/SHA256SUMS
artifacts/bin/cMVBT-v0.0.110-linux-x86_64-glibc238 ycsb --help
```

The executable and the surrounding repository are distributed under the
Apache License, Version 2.0. Third-party components linked into the executable
remain subject to their respective licenses.
