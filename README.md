# Aggregated Highest Random Weight Hashing / Aggregated Rendezvous Hashing

[![Go Reference](https://pkg.go.dev/badge/github.com/SenseUnit/ahrw.svg)](https://pkg.go.dev/github.com/SenseUnit/ahrw)

AHRW pre-aggregates input objects into specified number of slots and only then distributes these slots across provided nodes. It allows memoization of calculations made for each slot and makes rendezvous hashing practical for large numbers of nodes (or shares of capacity for weighted case) and/or high request rates.
