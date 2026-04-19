# Technical Design for Tickhouse
This document introduces the high-level technical design for the Tickhouse project.

## High Level
There are 2 main components
* The tickhouse server (`/tickhouse`) which stores the main storage and command/query execution logic.
* The tickhouse repl (`/tickhouse-repl`) which provides a REPL for the user to submit commands and queries to the tickhouse server
* They communicate via a TCP connection

## Server Components
The Server will make use of Clean Architecture, with business logic separated from implementation details like
* TCP request parsing
* Writing to files

### Domain Model

There is a `TickhouseService` that takes in parsed user commands/queries and delegates them to the appropriate tables. It has 3 methods:
* `create`
* `insert`
* `query`

There is an abstraction over the storage layer called the `Table`, with 3 methods.
* `create`
* `insert`
* `query`

### Storage Engine
* This will have concrete implementations of the `Table`, they should adhere to the functional and non-functional requirements in `prd.md`.
* The implementations should follow the iterations given in `plans.md`, starting with the MVP and iterating on it, performing benchmarks at each step.

### Query Parser
This will
* Define a binary protocol used for communication between the REPL and the server.
* Translate from that binary protocol to the appropriate method calls on `Tickhouse Service`

