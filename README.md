# pg_repack Isolation Test Suite

**Note: This project was generated with the assistance of AI.**

## Overview

This project contains a test suite to verify privilege isolation in PostgreSQL's `pg_repack` extension when used by multiple non-superuser accounts. It ensures that users cannot access intermediate tables created during another user's repack operation.

The test suite validates that `pg_repack` properly isolates intermediate tables and log tables through:
- Transaction isolation (MVCC) during the initial data copy phase
- Table ownership permissions after the copy completes
- Schema-level access controls

The suite creates three PostgreSQL users and verifies:
1. Users with repack privileges cannot access another user's intermediate tables
2. Users without repack privileges cannot access any repack-related tables
3. Metadata visibility doesn't grant data access

Example results can be viewed in the file [test_multiuser_isolation.log](test_multiuser_isolation.log)

## Usage

```bash
# Setup
./test_multiuser_isolation_setup.sh

# Run tests
./test_multiuser_isolation.sh

# Cleanup
./test_multiuser_isolation_cleanup.sh
```

## Requirements

- PostgreSQL with `pg_repack` extension installed
- `pgbench` for workload generation
- Bash shell

## License

Apache License 2.0

