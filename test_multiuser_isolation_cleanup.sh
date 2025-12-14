#!/bin/bash
# Cleanup script for pg_repack multi-user privilege isolation test
# This script removes all test artifacts created during testing

set -e

DB_NAME="repack_isolation_test"
USER1="repack_user1"
USER2="repack_user2"
USER3="no_repack_user"

echo "=== Cleaning up multi-user isolation test environment ==="

# Check if we can connect to PostgreSQL
if ! psql -d postgres -c "SELECT 1;" > /dev/null 2>&1; then
    echo "ERROR: Cannot connect to PostgreSQL database"
    echo "Please ensure PostgreSQL is running and peer authentication is configured"
    exit 1
fi
echo "✓ Connected to PostgreSQL"

# Check if any pg_repack processes are running for this database
echo "Checking for running pg_repack processes..."
if pgrep -f "pg_repack.*${DB_NAME}" > /dev/null; then
    echo "Warning: Found running pg_repack processes for ${DB_NAME}"
    echo "Attempting to terminate them..."
    pkill -f "pg_repack.*${DB_NAME}" || true
    sleep 2
    
    # Force kill if still running
    if pgrep -f "pg_repack.*${DB_NAME}" > /dev/null; then
        echo "Force killing processes..."
        pkill -9 -f "pg_repack.*${DB_NAME}" || true
        sleep 1
    fi
    echo "✓ Terminated pg_repack processes"
fi

# Terminate any active connections to the test database
echo "Terminating active connections to ${DB_NAME}..."
psql -d postgres <<EOF 2>/dev/null || true
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = '${DB_NAME}'
  AND pid <> pg_backend_pid();
EOF
sleep 1

# Drop test database
echo "Dropping test database: ${DB_NAME}"
if psql -d postgres -c "DROP DATABASE IF EXISTS ${DB_NAME};" 2>/dev/null; then
    echo "  ✓ Dropped database ${DB_NAME}"
else
    echo "  ! Database ${DB_NAME} does not exist or already dropped"
fi

# Drop test users
echo "Dropping test users..."
if psql -d postgres -c "DROP ROLE IF EXISTS ${USER1};" 2>/dev/null; then
    echo "  ✓ Dropped user ${USER1}"
else
    echo "  ! User ${USER1} does not exist or already dropped"
fi

if psql -d postgres -c "DROP ROLE IF EXISTS ${USER2};" 2>/dev/null; then
    echo "  ✓ Dropped user ${USER2}"
else
    echo "  ! User ${USER2} does not exist or already dropped"
fi

if psql -d postgres -c "DROP ROLE IF EXISTS ${USER3};" 2>/dev/null; then
    echo "  ✓ Dropped user ${USER3}"
else
    echo "  ! User ${USER3} does not exist or already dropped"
fi

# Remove log files
echo "Removing log files..."
rm -f test_multiuser_isolation.log
rm -f repack_output.log
echo "  ✓ Removed log files"

echo ""
echo "=== Cleanup complete ==="
echo ""
echo "All test artifacts have been removed:"
echo "  • Database ${DB_NAME}"
echo "  • Users: ${USER1}, ${USER2}, ${USER3}"
echo "  • Log files"
echo ""
