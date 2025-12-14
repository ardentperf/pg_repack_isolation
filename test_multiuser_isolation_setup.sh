#!/bin/bash
# Setup test environment for pg_repack multi-user privilege isolation testing
# This script must be run as a superuser (ubuntu user with peer auth)

set -e

DB_NAME="repack_isolation_test"
USER1="repack_user1"
USER2="repack_user2"
USER3="no_repack_user"
PASS1="repack_pass1_123"
PASS2="repack_pass2_123"
PASS3="no_repack_pass_123"
SCHEMA1="user1_schema"
SCHEMA2="user2_schema"
SCHEMA3="user3_schema"
ROW_COUNT=10000000  # 10 million rows for longer repack duration

echo "=== Setting up multi-user isolation test environment ==="

# Check if we can connect to PostgreSQL
if ! psql -d postgres -c "SELECT 1;" > /dev/null 2>&1; then
    echo "ERROR: Cannot connect to PostgreSQL database"
    echo "Please ensure PostgreSQL is running and peer authentication is configured"
    exit 1
fi
echo "✓ Connected to PostgreSQL"

# Drop existing test database and users if they exist (for clean slate)
echo "Cleaning up any existing test artifacts..."
psql -d postgres -c "DROP DATABASE IF EXISTS ${DB_NAME};" 2>/dev/null || true
psql -d postgres -c "DROP ROLE IF EXISTS ${USER1};" 2>/dev/null || true
psql -d postgres -c "DROP ROLE IF EXISTS ${USER2};" 2>/dev/null || true
psql -d postgres -c "DROP ROLE IF EXISTS ${USER3};" 2>/dev/null || true

# Create test users
echo "Creating test users..."
psql -d postgres -c "CREATE ROLE ${USER1} WITH LOGIN PASSWORD '${PASS1}';"
echo "  ✓ Created ${USER1}"
psql -d postgres -c "CREATE ROLE ${USER2} WITH LOGIN PASSWORD '${PASS2}';"
echo "  ✓ Created ${USER2}"
psql -d postgres -c "CREATE ROLE ${USER3} WITH LOGIN PASSWORD '${PASS3}';"
echo "  ✓ Created ${USER3}"

# Create test database
echo "Creating test database: ${DB_NAME}"
psql -d postgres -c "CREATE DATABASE ${DB_NAME};"

# Connect to test database and set up infrastructure
echo "Setting up database infrastructure..."

# Install pg_repack extension (requires superuser)
psql -d ${DB_NAME} <<EOF
-- Install pg_repack extension
CREATE EXTENSION IF NOT EXISTS pg_repack;

-- Grant repack schema privileges to repack_user1 and repack_user2 ONLY
-- no_repack_user will NOT get these privileges
GRANT USAGE, CREATE ON SCHEMA repack TO ${USER1};
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA repack TO ${USER1};
GRANT SELECT ON ALL TABLES IN SCHEMA repack TO ${USER1};
ALTER DEFAULT PRIVILEGES IN SCHEMA repack GRANT EXECUTE ON FUNCTIONS TO ${USER1};
ALTER DEFAULT PRIVILEGES IN SCHEMA repack GRANT SELECT ON TABLES TO ${USER1};

GRANT USAGE, CREATE ON SCHEMA repack TO ${USER2};
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA repack TO ${USER2};
GRANT SELECT ON ALL TABLES IN SCHEMA repack TO ${USER2};
ALTER DEFAULT PRIVILEGES IN SCHEMA repack GRANT EXECUTE ON FUNCTIONS TO ${USER2};
ALTER DEFAULT PRIVILEGES IN SCHEMA repack GRANT SELECT ON TABLES TO ${USER2};

-- Note: ${USER3} intentionally does NOT get repack privileges

EOF

echo "  ✓ pg_repack extension installed"
echo "  ✓ Granted repack privileges to ${USER1} and ${USER2}"
echo "  ✓ ${USER3} has NO repack privileges (by design)"

# Create schemas for each user
echo "Creating user schemas..."
psql -d ${DB_NAME} <<EOF
CREATE SCHEMA ${SCHEMA1} AUTHORIZATION ${USER1};
CREATE SCHEMA ${SCHEMA2} AUTHORIZATION ${USER2};
CREATE SCHEMA ${SCHEMA3} AUTHORIZATION ${USER3};
EOF

echo "  ✓ Created ${SCHEMA1} (owned by ${USER1})"
echo "  ✓ Created ${SCHEMA2} (owned by ${USER2})"
echo "  ✓ Created ${SCHEMA3} (owned by ${USER3})"

# Create tables and data for user1 (the one we'll repack)
echo "Creating test table for ${USER1}..."
PGPASSWORD=${PASS1} psql -h localhost -U ${USER1} -d ${DB_NAME} <<EOF
-- Create source table with fillfactor=70 for testing
CREATE TABLE ${SCHEMA1}.source_data (
    id SERIAL PRIMARY KEY,
    data TEXT,
    created_at TIMESTAMP DEFAULT NOW()
) WITH (fillfactor=70);

-- Insert ${ROW_COUNT} rows
-- Initialize data field to same length as update payload ('Updated-' + 8 digits = 16 chars)
-- This ensures updates don't change row size, enabling HOT (Heap-Only Tuple) updates
INSERT INTO ${SCHEMA1}.source_data (data)
SELECT 
    'Initial-' || lpad(i::text, 8, '0')  -- 'Initial-00000001' = 16 chars, matches 'Updated-12345678'
FROM generate_series(1, ${ROW_COUNT}) i;

EOF

echo "  ✓ Created ${SCHEMA1}.source_data with ${ROW_COUNT} rows (fillfactor=70)"
echo "  ✓ Data field initialized to same length as update payload (enables HOT updates)"

# Create simple tables for user2 and user3 (for completeness)
echo "Creating test tables for ${USER2} and ${USER3}..."
PGPASSWORD=${PASS2} psql -h localhost -U ${USER2} -d ${DB_NAME} <<EOF
CREATE TABLE ${SCHEMA2}.source_data (
    id SERIAL PRIMARY KEY,
    data TEXT
);
INSERT INTO ${SCHEMA2}.source_data (data)
SELECT 'User2 Row ' || i FROM generate_series(1, 100) i;
EOF

PGPASSWORD=${PASS3} psql -h localhost -U ${USER3} -d ${DB_NAME} <<EOF
CREATE TABLE ${SCHEMA3}.source_data (
    id SERIAL PRIMARY KEY,
    data TEXT
);
INSERT INTO ${SCHEMA3}.source_data (data)
SELECT 'User3 Row ' || i FROM generate_series(1, 100) i;
EOF

echo "  ✓ Created ${SCHEMA2}.source_data with 100 rows"
echo "  ✓ Created ${SCHEMA3}.source_data with 100 rows"

# Verify isolation: no user can read other users' tables
echo ""
echo "Verifying privilege isolation..."
psql -d ${DB_NAME} <<EOF
-- Ensure no cross-user table access (should show no grants)
-- Each user should only be able to access their own schema
SELECT 
    schemaname, 
    tablename, 
    tableowner
FROM pg_tables 
WHERE schemaname IN ('${SCHEMA1}', '${SCHEMA2}', '${SCHEMA3}')
ORDER BY schemaname;
EOF

echo ""
echo "=== Test environment setup complete ==="
echo ""
echo "Database: ${DB_NAME}"
echo ""
echo "Users and capabilities:"
echo "  • ${USER1} - CAN run pg_repack, owns ${SCHEMA1}"
echo "  • ${USER2} - CAN run pg_repack, owns ${SCHEMA2}"
echo "  • ${USER3} - CANNOT run pg_repack, owns ${SCHEMA3}"
echo ""
echo "Key constraint: No user has SELECT privilege on other users' tables"
echo ""
echo "Test table: ${SCHEMA1}.source_data"
echo "  - ${ROW_COUNT} rows"
echo "  - fillfactor=70 (30% free space for HOT updates)"
echo "  - Data field: 16 chars ('Initial-00000001' → 'Updated-12345678')"
echo "  - Row size constant during updates (enables in-place HOT updates)"
echo "  - Repack will take several minutes, allowing time for concurrent tests"
echo ""

