# Product Requirements Document for Tickhouse
This document outlines the requirements for the Tickhouse project.

## Problem Statement
Historical Market Data can have huge volumes, especially for assets like options.

This presents a challenge when trying to store them, as sometimes the rate at which we store new data exceeds the rate at which we can obtain new on-prem storage servers.

However, there are characteristics of that we can capitalise on to achieve greater compression than normal compression algorithms or general-purpose Time Series Databases. For instance:
* price movements are (usually) relatively small
* queries are usually done by symbol, then by time

Hence, this project will experiment with various ways of compression, including but not limited to:
* XOR floating point compression
* delta compression
* delta of delta compression
* etc.

## Functional Requirements
* A user should be able to create a new table with SQL-like syntax
    * e.g. `CREATE TABLE us_stocks_table`
* A user should be able to insert Bar Data into the database (intraday would be ideal but I don't want to pay for an API key). This will include
    * `date`
    * `symbol`
    * `open`
    * `high`
    * `low`
    * `close`
    * `volume`
* A user should be able to query by symbol and date with SQL-like syntax
    * e.g. `SELECT date, high, low FROM us_stocks_table WHERE symbol = 'AAPL' AND date >= '2026-01-01'`
* The user should be able to access those functions through a REPL similar to `psql`

## Non-Functional Requirements
* The main aim is to minimise total size of the raw data files as much as possible. The target is to achieve a compression ratio that beats [Clickhouse](https://clickhouse.com)
* The queries should still have reasonable performance. Target is < 1s for a table with 1 million bars.

