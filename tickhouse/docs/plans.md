# Plans for Tickhouse
This document tracks the planned iterations for Tickhouse

**Benchmarking**

Between each iteration, simple benchmarks should be done. We can:
* Set up a fixed dummy data file (e.g. 1 million records in JSON format)
* `INSERT` it into tickhouse
* Ensure queries take < 1s
* Check the storage cost (sum of the sizes of all the data files)

## MVP
- [ ] Done

* Naive Implementation with Parquet files

## Custom Column-Store
- [ ] Done

* Based on [Clickhouse MergeTree](https://clickhouse.com/docs/development/architecture#merge-tree)
* Separate files per column
* Use a sparse index on the compressed files

## Delta Compression
- [ ] Done

* Try delta compression on columns that don't change much

## Delta of Deltas Compression
- [ ] Done

* Try delta of deltas compression on columns that don't change much

## Delta of Deltas of Deltas Compression
- [ ] Done

* It could be funny lol

## XOR Floating point compression
- [ ] Done

* Try on float values

## Others
* Explore any other ideas that pop up
