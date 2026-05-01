# tickhouse

A columnar, time-series database for highly-compressed storage of L1 Market Data Ticks.

# Rationale
This project serves to experiment with the compression ratios of various storage solutions, as well as compression methods.

# Comparisons
| setup | size (bytes) | size (relative to raw data) |
| :--- | :--- | :---- |
| naive JSON | 8003616 | 1.00x |
| Parquet | 1076515 | 0.135x |
