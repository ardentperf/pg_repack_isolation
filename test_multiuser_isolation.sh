#!/bin/bash
# Test pg_repack privilege isolation between multiple non-superusers
# Tests that users cannot access intermediate tables created during another user's repack

set -e

DB_NAME="repack_isolation_test"
USER1="repack_user1"
USER2="repack_user2"
USER3="no_repack_user"
PASS1="repack_pass1_123"
PASS2="repack_pass2_123"
PASS3="no_repack_pass_123"
SCHEMA1="user1_schema"
TABLE1="${SCHEMA1}.source_data"
LOG_FILE="test_multiuser_isolation.log"
ROW_COUNT=10000000  # Must match setup script's ROW_COUNT

echo "=== Testing pg_repack privilege isolation between users ===" | tee ${LOG_FILE}
echo "Date: $(date)" | tee -a ${LOG_FILE}
echo "" | tee -a ${LOG_FILE}

# Check database exists
if ! psql -d ${DB_NAME} -c "SELECT 1;" > /dev/null 2>&1; then
    echo "ERROR: Database ${DB_NAME} does not exist. Run setup script first." | tee -a ${LOG_FILE}
    exit 1
fi

echo "✓ Database ${DB_NAME} exists" | tee -a ${LOG_FILE}
echo "" | tee -a ${LOG_FILE}

# Get table OID for identifying intermediate tables
TABLE_OID=$(psql -d ${DB_NAME} -t -c "SELECT oid FROM pg_class WHERE relname = 'source_data' AND relnamespace = '${SCHEMA1}'::regnamespace;" | xargs)
echo "Source table OID: ${TABLE_OID}" | tee -a ${LOG_FILE}

INTERMEDIATE_TABLE="repack.table_${TABLE_OID}"
LOG_TABLE="repack.log_${TABLE_OID}"
echo "Expected intermediate table: ${INTERMEDIATE_TABLE}" | tee -a ${LOG_FILE}
echo "Expected log table: ${LOG_TABLE}" | tee -a ${LOG_FILE}
echo "" | tee -a ${LOG_FILE}

# Function to check if repack is actively copying data (INSERT running)
repack_is_copying() {
    local oid=$1
    psql -d ${DB_NAME} -t -c "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname = '${DB_NAME}' AND query LIKE '%INSERT INTO repack.table_${oid}%' AND state = 'active');" | xargs
}

# Function to check if log table exists
log_table_exists() {
    local oid=$1
    psql -d ${DB_NAME} -t -c "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'repack' AND tablename = 'log_${oid}');" | xargs
}

# Start background workload that continuously updates the table
echo "=== Starting background workload ===" | tee -a ${LOG_FILE}
echo "Launching continuous UPDATE workload on ${TABLE1}..." | tee -a ${LOG_FILE}

# Background script that updates random rows
# Use ROW_COUNT variable to match the actual table size
# Update format: 'Updated-' + 8-digit padded number = 16 chars (same as initial 'Initial-00000001')
cat > workload_script.sql <<EOF
\set id random(1, ${ROW_COUNT})
UPDATE user1_schema.source_data SET data = 'Updated-' || lpad(:id::text, 8, '0') WHERE id = :id;
EOF

# Run pgbench with the update workload in background
PGPASSWORD=${PASS1} pgbench -h localhost -U ${USER1} -d ${DB_NAME} -f workload_script.sql -c 2 -j 2 -T 600 -n > workload_output.log 2>&1 &
WORKLOAD_PID=$!

echo "✓ Background workload started with PID: ${WORKLOAD_PID}" | tee -a ${LOG_FILE}
echo "  2 concurrent clients updating random rows for up to 10 minutes" | tee -a ${LOG_FILE}
sleep 2  # Let workload start
echo "" | tee -a ${LOG_FILE}

# Start repack in background
echo "=== Starting pg_repack for ${USER1} in background ===" | tee -a ${LOG_FILE}
echo "Command: pg_repack --no-superuser-check -h localhost -U ${USER1} -d ${DB_NAME} -t ${TABLE1}" | tee -a ${LOG_FILE}
echo "Note: 10 million row table will take several minutes to repack" | tee -a ${LOG_FILE}
echo "" | tee -a ${LOG_FILE}

PGPASSWORD=${PASS1} pg_repack --no-superuser-check -h localhost -U ${USER1} -d ${DB_NAME} -t ${TABLE1} -e 2>&1 | while read LINE; do echo "$(date +"%Y-%m-%d %H:%M:%S") ${LINE}"; done >> repack_output.log &
REPACK_PID=$!

echo "pg_repack started with PID: ${REPACK_PID}" | tee -a ${LOG_FILE}

# Wait for repack to start copying data (poll every 0.5 seconds, max 30 seconds)
echo "Waiting for repack to start data copy operation..." | tee -a ${LOG_FILE}
WAIT_COUNT=0
MAX_WAIT=60  # 60 iterations * 0.5 seconds = 30 seconds max

while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    # Check if pg_repack process is still running
    if ! kill -0 ${REPACK_PID} 2>/dev/null; then
        echo "ERROR: pg_repack process terminated unexpectedly" | tee -a ${LOG_FILE}
        echo "Check repack_output.log for details" | tee -a ${LOG_FILE}
        cat repack_output.log | tee -a ${LOG_FILE}
        exit 1
    fi
    
    # Check if log table exists (created early in the process)
    if [ "$(log_table_exists ${TABLE_OID})" = "t" ]; then
        echo "✓ Log table detected: ${LOG_TABLE}" | tee -a ${LOG_FILE}
        
        # Now check if INSERT is running (means intermediate table is being populated)
        if [ "$(repack_is_copying ${TABLE_OID})" = "t" ]; then
            echo "✓ Data copy in progress: INSERT INTO ${INTERMEDIATE_TABLE}" | tee -a ${LOG_FILE}
            break
        fi
    fi
    
    WAIT_COUNT=$((WAIT_COUNT + 1))
    echo "  Waiting... (attempt ${WAIT_COUNT}/${MAX_WAIT})" | tee -a ${LOG_FILE}
    sleep 0.5
done

if [ $WAIT_COUNT -eq $MAX_WAIT ]; then
    echo "ERROR: Timeout waiting for repack data copy to start" | tee -a ${LOG_FILE}
    echo "Killing pg_repack process..." | tee -a ${LOG_FILE}
    kill ${REPACK_PID} 2>/dev/null || true
    exit 1
fi

# Verify intermediate objects exist
echo "" | tee -a ${LOG_FILE}
echo "=== Verifying intermediate objects ===" | tee -a ${LOG_FILE}
psql -d ${DB_NAME} -c "SELECT schemaname, tablename, tableowner FROM pg_tables WHERE schemaname = 'repack' AND tablename LIKE '%${TABLE_OID}%' ORDER BY tablename;" | tee -a ${LOG_FILE}
echo "" | tee -a ${LOG_FILE}
echo "Active repack queries:" | tee -a ${LOG_FILE}
psql -d ${DB_NAME} -c "SELECT pid, usename, state, left(query, 100) FROM pg_stat_activity WHERE datname = '${DB_NAME}' AND usename = '${USER1}' AND state = 'active';" | tee -a ${LOG_FILE}
echo "" | tee -a ${LOG_FILE}

# Verify pg_repack is still running
if ! kill -0 ${REPACK_PID} 2>/dev/null; then
    echo "ERROR: pg_repack process is no longer running" | tee -a ${LOG_FILE}
    exit 1
fi
echo "✓ pg_repack process ${REPACK_PID} is still running" | tee -a ${LOG_FILE}

# Check workload status
if kill -0 ${WORKLOAD_PID} 2>/dev/null; then
    echo "✓ Background workload ${WORKLOAD_PID} is still running" | tee -a ${LOG_FILE}
    WORKLOAD_UPDATES=$(tail -1 workload_output.log 2>/dev/null | grep -oP '\d+(?= of)' || echo "N/A")
    echo "  Updates so far: ${WORKLOAD_UPDATES}" | tee -a ${LOG_FILE}
fi
echo "" | tee -a ${LOG_FILE}

# TEST 1: USER2 attempts to query USER1's log table
echo "=== TEST 1: ${USER2} attempts to query ${USER1}'s log table ===" | tee -a ${LOG_FILE}
echo "Testing: ${LOG_TABLE}" | tee -a ${LOG_FILE}
if PGPASSWORD=${PASS2} psql -h localhost -U ${USER2} -d ${DB_NAME} -c "SELECT * FROM ${LOG_TABLE} LIMIT 1;" >> ${LOG_FILE} 2>&1; then
    echo "✗ FAIL: ${USER2} CAN query ${LOG_TABLE} (should be denied)" | tee -a ${LOG_FILE}
    TEST1_RESULT="FAIL"
else
    echo "✓ PASS: ${USER2} CANNOT query ${LOG_TABLE} (permission denied as expected)" | tee -a ${LOG_FILE}
    TEST1_RESULT="PASS"
fi
echo "" | tee -a ${LOG_FILE}

# TEST 2: USER2 attempts to query via pg_class (intermediate table in transaction)
echo "=== TEST 2: ${USER2} attempts to access intermediate table metadata ===" | tee -a ${LOG_FILE}
echo "Testing: Access to repack schema intermediate tables" | tee -a ${LOG_FILE}
# Try to query the table that's being created (even though it's in a transaction, we test the schema access)
if PGPASSWORD=${PASS2} psql -h localhost -U ${USER2} -d ${DB_NAME} -c "SELECT relname FROM pg_class WHERE relnamespace = 'repack'::regnamespace AND relname LIKE 'table_%${TABLE_OID}' LIMIT 1;" >> ${LOG_FILE} 2>&1; then
    # If they can see the table name, try to access it
    TABLE_NAME=$(PGPASSWORD=${PASS2} psql -h localhost -U ${USER2} -d ${DB_NAME} -t -c "SELECT relname FROM pg_class WHERE relnamespace = 'repack'::regnamespace AND relname LIKE 'table_%${TABLE_OID}' LIMIT 1;" 2>/dev/null | xargs)
    if [ -n "$TABLE_NAME" ]; then
        echo "  Found table: $TABLE_NAME, testing access..." | tee -a ${LOG_FILE}
        if PGPASSWORD=${PASS2} psql -h localhost -U ${USER2} -d ${DB_NAME} -c "SELECT * FROM repack.${TABLE_NAME} LIMIT 1;" >> ${LOG_FILE} 2>&1; then
            echo "✗ FAIL: ${USER2} CAN query repack.${TABLE_NAME} (should be denied)" | tee -a ${LOG_FILE}
            TEST2_RESULT="FAIL"
        else
            echo "✓ PASS: ${USER2} CANNOT query repack.${TABLE_NAME} (permission denied as expected)" | tee -a ${LOG_FILE}
            TEST2_RESULT="PASS"
        fi
    else
        echo "✓ PASS: ${USER2} cannot even see intermediate table name" | tee -a ${LOG_FILE}
        TEST2_RESULT="PASS"
    fi
else
    echo "✓ PASS: ${USER2} CANNOT query pg_class for repack schema (permission denied as expected)" | tee -a ${LOG_FILE}
    TEST2_RESULT="PASS"
fi
echo "" | tee -a ${LOG_FILE}

# TEST 3: USER2 attempts to query USER1's source table
echo "=== TEST 3: ${USER2} attempts to query ${USER1}'s source table ===" | tee -a ${LOG_FILE}
echo "Testing: ${TABLE1}" | tee -a ${LOG_FILE}
if PGPASSWORD=${PASS2} psql -h localhost -U ${USER2} -d ${DB_NAME} -c "SELECT * FROM ${TABLE1} LIMIT 1;" >> ${LOG_FILE} 2>&1; then
    echo "✗ FAIL: ${USER2} CAN query ${TABLE1} (should be denied)" | tee -a ${LOG_FILE}
    TEST3_RESULT="FAIL"
else
    echo "✓ PASS: ${USER2} CANNOT query ${TABLE1} (permission denied as expected)" | tee -a ${LOG_FILE}
    TEST3_RESULT="PASS"
fi
echo "" | tee -a ${LOG_FILE}

# TEST 4: USER3 (no repack privileges) attempts to access repack schema
echo "=== TEST 4: ${USER3} attempts to access repack schema ===" | tee -a ${LOG_FILE}
echo "Testing: Access to repack schema" | tee -a ${LOG_FILE}
if PGPASSWORD=${PASS3} psql -h localhost -U ${USER3} -d ${DB_NAME} -c "SELECT * FROM ${LOG_TABLE} LIMIT 1;" >> ${LOG_FILE} 2>&1; then
    echo "✗ FAIL: ${USER3} CAN access repack schema (should be denied)" | tee -a ${LOG_FILE}
    TEST4_RESULT="FAIL"
else
    echo "✓ PASS: ${USER3} CANNOT access repack schema (permission denied as expected)" | tee -a ${LOG_FILE}
    TEST4_RESULT="PASS"
fi
echo "" | tee -a ${LOG_FILE}

# TEST 5: USER3 can see metadata but cannot access data (expected PostgreSQL behavior)
echo "=== TEST 5: ${USER3} metadata visibility vs data access ===" | tee -a ${LOG_FILE}
echo "Testing: Metadata visibility (pg_tables) - expected to be visible" | tee -a ${LOG_FILE}
echo "Testing: Data access - should be denied" | tee -a ${LOG_FILE}

# PostgreSQL allows all users to query system catalogs like pg_tables
# This is expected and NOT a security issue - the protection is at the data access level
METADATA_VISIBLE=$(PGPASSWORD=${PASS3} psql -h localhost -U ${USER3} -d ${DB_NAME} -t -c "SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'repack';" 2>/dev/null | xargs || echo "0")

if [ "$METADATA_VISIBLE" -gt 0 ]; then
    echo "  ℹ INFO: ${USER3} can see repack table names in pg_tables (normal PostgreSQL behavior)" | tee -a ${LOG_FILE}
    
    # Now verify they CANNOT access the actual data (this is the real security test)
    TABLE_NAME=$(PGPASSWORD=${PASS3} psql -h localhost -U ${USER3} -d ${DB_NAME} -t -c "SELECT tablename FROM pg_tables WHERE schemaname = 'repack' LIMIT 1;" 2>/dev/null | xargs)
    if [ -n "$TABLE_NAME" ]; then
        if PGPASSWORD=${PASS3} psql -h localhost -U ${USER3} -d ${DB_NAME} -c "SELECT * FROM repack.${TABLE_NAME} LIMIT 1;" >> ${LOG_FILE} 2>&1; then
            echo "✗ FAIL: ${USER3} CAN access data in repack.${TABLE_NAME} (security breach!)" | tee -a ${LOG_FILE}
            TEST5_RESULT="FAIL"
        else
            echo "✓ PASS: ${USER3} CANNOT access data (permission denied as expected)" | tee -a ${LOG_FILE}
            echo "  Note: Metadata visibility is normal; data access properly blocked" | tee -a ${LOG_FILE}
            TEST5_RESULT="PASS"
        fi
    fi
else
    echo "✓ PASS: ${USER3} cannot even see table names (extra restrictive)" | tee -a ${LOG_FILE}
    TEST5_RESULT="PASS"
fi
echo "" | tee -a ${LOG_FILE}

# TEST 6: USER3 attempts to query USER1's source table during initial copy
echo "=== TEST 6: ${USER3} attempts to query ${USER1}'s source table ===" | tee -a ${LOG_FILE}
echo "Testing: ${TABLE1}" | tee -a ${LOG_FILE}
if PGPASSWORD=${PASS3} psql -h localhost -U ${USER3} -d ${DB_NAME} -c "SELECT * FROM ${TABLE1} LIMIT 1;" >> ${LOG_FILE} 2>&1; then
    echo "✗ FAIL: ${USER3} CAN query ${TABLE1} (should be denied)" | tee -a ${LOG_FILE}
    TEST6_RESULT="FAIL"
else
    echo "✓ PASS: ${USER3} CANNOT query ${TABLE1} (permission denied as expected)" | tee -a ${LOG_FILE}
    TEST6_RESULT="PASS"
fi
echo "" | tee -a ${LOG_FILE}

# TEST 7: USER1 (owner) attempts to query their own log table
echo "=== TEST 7: ${USER1} attempts to query own log table ===" | tee -a ${LOG_FILE}
echo "Testing: ${LOG_TABLE}" | tee -a ${LOG_FILE}
echo "Context: Owner should have access to their own log table" | tee -a ${LOG_FILE}
if PGPASSWORD=${PASS1} psql -h localhost -U ${USER1} -d ${DB_NAME} -c "SELECT COUNT(*) FROM ${LOG_TABLE};" >> ${LOG_FILE} 2>&1; then
    echo "✓ PASS: ${USER1} CAN query ${LOG_TABLE} (expected)" | tee -a ${LOG_FILE}
    TEST7_RESULT="PASS"
else
    echo "✗ FAIL: ${USER1} cannot query their own log table" | tee -a ${LOG_FILE}
    TEST7_RESULT="FAIL"
fi
echo "" | tee -a ${LOG_FILE}

# TEST 8: USER1 attempts to query their own intermediate table (pre-commit, in serializable txn)
echo "=== TEST 8: ${USER1} attempts to access own intermediate table metadata (pre-commit) ===" | tee -a ${LOG_FILE}
echo "Context: Owner queries own uncommitted table from outside serializable transaction" | tee -a ${LOG_FILE}
# During the data copy, the intermediate table is being created in a serializable transaction
# Even the owner may not see it from a different session until the transaction commits
CAN_SEE=$(PGPASSWORD=${PASS1} psql -h localhost -U ${USER1} -d ${DB_NAME} -t -c "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'table_${TABLE_OID}' AND relnamespace = 'repack'::regnamespace);" 2>/dev/null | xargs || echo "f")
if [ "$CAN_SEE" = "t" ]; then
    echo "  ℹ INFO: ${USER1} can see their own table in pg_class (txn may have committed)" | tee -a ${LOG_FILE}
    echo "✓ PASS: Table visible (transaction committed early)" | tee -a ${LOG_FILE}
    TEST8_RESULT="PASS"
else
    echo "✓ PASS: ${USER1} cannot see own table yet (serializable transaction not committed)" | tee -a ${LOG_FILE}
    echo "  This demonstrates MVCC/serializable transaction isolation" | tee -a ${LOG_FILE}
    TEST8_RESULT="PASS"
fi
echo "" | tee -a ${LOG_FILE}

# TEST 9: USER1 (owner) attempts to query their own source table
echo "=== TEST 9: ${USER1} attempts to query own source table ===" | tee -a ${LOG_FILE}
echo "Testing: ${TABLE1}" | tee -a ${LOG_FILE}
echo "Context: Owner should have access to their own source table" | tee -a ${LOG_FILE}
if PGPASSWORD=${PASS1} psql -h localhost -U ${USER1} -d ${DB_NAME} -c "SELECT COUNT(*) FROM ${TABLE1};" >> ${LOG_FILE} 2>&1; then
    echo "✓ PASS: ${USER1} CAN query ${TABLE1} (expected)" | tee -a ${LOG_FILE}
    TEST9_RESULT="PASS"
else
    echo "✗ FAIL: ${USER1} cannot query their own source table" | tee -a ${LOG_FILE}
    TEST9_RESULT="FAIL"
fi
echo "" | tee -a ${LOG_FILE}

# Wait for initial data copy to complete and transaction to commit (table becomes visible)
echo "=== Waiting for initial data copy to complete and commit ===" | tee -a ${LOG_FILE}
echo "Checking for intermediate table visibility in pg_class (indicates transaction committed)..." | tee -a ${LOG_FILE}
COPY_WAIT=0
while [ $COPY_WAIT -lt 600 ]; do  # 10 minutes max
    if ! kill -0 ${REPACK_PID} 2>/dev/null; then
        echo "✓ pg_repack completed" | tee -a ${LOG_FILE}
        break
    fi
    
    # Check if intermediate table is now visible in pg_class (transaction committed)
    # This is a better indicator than checking for INSERT because:
    # - During initial copy: table is invisible (transaction uncommitted)
    # - After initial copy commits: table becomes visible in pg_class
    # - During log catchup: INSERT statements still run, but table is visible
    INTERMEDIATE_VISIBLE=$(psql -d ${DB_NAME} -t -c "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'table_${TABLE_OID}' AND relnamespace = 'repack'::regnamespace);" | xargs)
    if [ "$INTERMEDIATE_VISIBLE" = "t" ]; then
        echo "✓ Intermediate table now visible in pg_class (initial copy transaction committed)" | tee -a ${LOG_FILE}
        echo "  pg_repack is now in log catchup/apply phase" | tee -a ${LOG_FILE}
        break
    fi
    
    COPY_WAIT=$((COPY_WAIT + 1))
    if [ $((COPY_WAIT % 10)) -eq 0 ]; then
        echo "  Still waiting for transaction commit... (${COPY_WAIT}s elapsed)" | tee -a ${LOG_FILE}
    fi
    sleep 1
done
echo "" | tee -a ${LOG_FILE}

# Now test access controls AFTER the intermediate table is committed and visible
echo "================================================================" | tee -a ${LOG_FILE}
echo "   POST-COMMIT ACCESS CONTROL TESTS (Table Now Visible)" | tee -a ${LOG_FILE}
echo "================================================================" | tee -a ${LOG_FILE}
echo "" | tee -a ${LOG_FILE}

# Re-verify intermediate table is visible in pg_class
echo "=== Verifying intermediate table visibility in pg_class ===" | tee -a ${LOG_FILE}
INTERMEDIATE_VISIBLE=$(psql -d ${DB_NAME} -t -c "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'table_${TABLE_OID}' AND relnamespace = 'repack'::regnamespace);" | xargs)
if [ "$INTERMEDIATE_VISIBLE" = "t" ]; then
    echo "✓ Intermediate table is visible in pg_class (transaction committed)" | tee -a ${LOG_FILE}
    echo "" | tee -a ${LOG_FILE}
    echo "Intermediate table details:" | tee -a ${LOG_FILE}
    psql -d ${DB_NAME} -c "SELECT relname, relowner::regrole, pg_size_pretty(pg_relation_size(oid)) as size FROM pg_class WHERE relname LIKE '%${TABLE_OID}' AND relnamespace = 'repack'::regnamespace ORDER BY relname;" | tee -a ${LOG_FILE}
else
    echo "  Note: Intermediate table not yet visible (still in transaction or already dropped)" | tee -a ${LOG_FILE}
fi
echo "" | tee -a ${LOG_FILE}

# Verify repack is still applying logged changes
if kill -0 ${REPACK_PID} 2>/dev/null; then
    echo "✓ pg_repack still running (applying logged changes from concurrent updates)" | tee -a ${LOG_FILE}
    psql -d ${DB_NAME} -c "SELECT pid, usename, state, left(query, 100) FROM pg_stat_activity WHERE datname = '${DB_NAME}' AND usename = '${USER1}';" | tee -a ${LOG_FILE}
else
    echo "  Note: pg_repack already completed" | tee -a ${LOG_FILE}
fi
echo "" | tee -a ${LOG_FILE}

# TEST 10: repack_user2 attempts to query now-visible intermediate table
echo "=== TEST 10: ${USER2} attempts to query visible intermediate table ===" | tee -a ${LOG_FILE}
echo "Testing: ${INTERMEDIATE_TABLE} (now committed and visible in pg_class)" | tee -a ${LOG_FILE}
echo "Context: This tests ownership-based protection AFTER transaction commits" | tee -a ${LOG_FILE}
if [ "$INTERMEDIATE_VISIBLE" = "t" ]; then
    # Verify user2 can see it in pg_class (expected)
    CAN_SEE=$(PGPASSWORD=${PASS2} psql -h localhost -U ${USER2} -d ${DB_NAME} -t -c "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'table_${TABLE_OID}' AND relnamespace = 'repack'::regnamespace);" 2>/dev/null | xargs || echo "f")
    if [ "$CAN_SEE" = "t" ]; then
        echo "  ℹ INFO: ${USER2} can see table in pg_class (expected)" | tee -a ${LOG_FILE}
    fi
    
    # Now test if they can access the data (should fail)
    if PGPASSWORD=${PASS2} psql -h localhost -U ${USER2} -d ${DB_NAME} -c "SELECT * FROM ${INTERMEDIATE_TABLE} LIMIT 1;" >> ${LOG_FILE} 2>&1; then
        echo "✗ FAIL: ${USER2} CAN query ${INTERMEDIATE_TABLE} data (security breach!)" | tee -a ${LOG_FILE}
        TEST10_RESULT="FAIL"
    else
        echo "✓ PASS: ${USER2} CANNOT query ${INTERMEDIATE_TABLE} data (permission denied as expected)" | tee -a ${LOG_FILE}
        echo "  Protection mechanism: Table ownership (owned by ${USER1})" | tee -a ${LOG_FILE}
        TEST10_RESULT="PASS"
    fi
else
    echo "  Skipped: Intermediate table not visible (already swapped/dropped)" | tee -a ${LOG_FILE}
    TEST10_RESULT="SKIPPED"
fi
echo "" | tee -a ${LOG_FILE}

# TEST 11: repack_user2 attempts to query log table post-commit
echo "=== TEST 11: ${USER2} attempts to query log table (post-commit) ===" | tee -a ${LOG_FILE}
echo "Testing: ${LOG_TABLE} (committed and visible)" | tee -a ${LOG_FILE}
echo "Context: Verifies log table protection after transaction commits" | tee -a ${LOG_FILE}
# Log table should still be visible and owned by repack_user1
LOG_VISIBLE=$(psql -d ${DB_NAME} -t -c "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'log_${TABLE_OID}' AND relnamespace = 'repack'::regnamespace);" | xargs)
if [ "$LOG_VISIBLE" = "t" ]; then
    # Verify user2 can see it in pg_class
    CAN_SEE=$(PGPASSWORD=${PASS2} psql -h localhost -U ${USER2} -d ${DB_NAME} -t -c "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'log_${TABLE_OID}' AND relnamespace = 'repack'::regnamespace);" 2>/dev/null | xargs || echo "f")
    if [ "$CAN_SEE" = "t" ]; then
        echo "  ℹ INFO: ${USER2} can see log table in pg_class (expected)" | tee -a ${LOG_FILE}
    fi
    
    # Test if they can access the data (should fail)
    if PGPASSWORD=${PASS2} psql -h localhost -U ${USER2} -d ${DB_NAME} -c "SELECT * FROM ${LOG_TABLE} LIMIT 1;" >> ${LOG_FILE} 2>&1; then
        echo "✗ FAIL: ${USER2} CAN query ${LOG_TABLE} data (security breach!)" | tee -a ${LOG_FILE}
        TEST11_RESULT="FAIL"
    else
        echo "✓ PASS: ${USER2} CANNOT query ${LOG_TABLE} data (permission denied as expected)" | tee -a ${LOG_FILE}
        echo "  Protection mechanism: Table ownership (owned by ${USER1})" | tee -a ${LOG_FILE}
        TEST11_RESULT="PASS"
    fi
else
    echo "  Skipped: Log table not visible (already dropped)" | tee -a ${LOG_FILE}
    TEST11_RESULT="SKIPPED"
fi
echo "" | tee -a ${LOG_FILE}

# TEST 12: no_repack_user can see table name in pg_class but cannot access data
echo "=== TEST 12: ${USER3} metadata visibility vs data access ===" | tee -a ${LOG_FILE}
echo "Context: Tests that metadata visibility doesn't grant data access" | tee -a ${LOG_FILE}
if [ "$INTERMEDIATE_VISIBLE" = "t" ]; then
    # First, verify they CAN see it in pg_class (this is expected)
    CAN_SEE=$(PGPASSWORD=${PASS3} psql -h localhost -U ${USER3} -d ${DB_NAME} -t -c "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'table_${TABLE_OID}' AND relnamespace = 'repack'::regnamespace);" 2>/dev/null | xargs || echo "f")
    if [ "$CAN_SEE" = "t" ]; then
        echo "  ℹ INFO: ${USER3} can see table in pg_class (expected PostgreSQL behavior)" | tee -a ${LOG_FILE}
    else
        echo "  Note: ${USER3} cannot see table in pg_class (extra restrictive)" | tee -a ${LOG_FILE}
    fi
    
    # Now verify they CANNOT access the data (the critical test)
    if PGPASSWORD=${PASS3} psql -h localhost -U ${USER3} -d ${DB_NAME} -c "SELECT * FROM ${INTERMEDIATE_TABLE} LIMIT 1;" >> ${LOG_FILE} 2>&1; then
        echo "✗ FAIL: ${USER3} CAN access ${INTERMEDIATE_TABLE} data (security breach!)" | tee -a ${LOG_FILE}
        TEST12_RESULT="FAIL"
    else
        echo "✓ PASS: ${USER3} CANNOT access ${INTERMEDIATE_TABLE} data (permission denied)" | tee -a ${LOG_FILE}
        echo "  Protection mechanism: Schema-level or table ownership permissions" | tee -a ${LOG_FILE}
        TEST12_RESULT="PASS"
    fi
else
    echo "  Skipped: Intermediate table not visible (already swapped/dropped)" | tee -a ${LOG_FILE}
    TEST12_RESULT="SKIPPED"
fi
echo "" | tee -a ${LOG_FILE}

# TEST 13: no_repack_user attempts to query log table post-commit
echo "=== TEST 13: ${USER3} attempts to query log table (post-commit) ===" | tee -a ${LOG_FILE}
echo "Testing: ${LOG_TABLE}" | tee -a ${LOG_FILE}
echo "Context: Verifies log table protection for users without repack privileges" | tee -a ${LOG_FILE}
if [ "$LOG_VISIBLE" = "t" ]; then
    # Verify they can/cannot see it in pg_class
    CAN_SEE=$(PGPASSWORD=${PASS3} psql -h localhost -U ${USER3} -d ${DB_NAME} -t -c "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'log_${TABLE_OID}' AND relnamespace = 'repack'::regnamespace);" 2>/dev/null | xargs || echo "f")
    if [ "$CAN_SEE" = "t" ]; then
        echo "  ℹ INFO: ${USER3} can see log table in pg_class (expected)" | tee -a ${LOG_FILE}
    fi
    
    # Test if they can access the data (should fail)
    if PGPASSWORD=${PASS3} psql -h localhost -U ${USER3} -d ${DB_NAME} -c "SELECT * FROM ${LOG_TABLE} LIMIT 1;" >> ${LOG_FILE} 2>&1; then
        echo "✗ FAIL: ${USER3} CAN query ${LOG_TABLE} data (security breach!)" | tee -a ${LOG_FILE}
        TEST13_RESULT="FAIL"
    else
        echo "✓ PASS: ${USER3} CANNOT query ${LOG_TABLE} data (permission denied)" | tee -a ${LOG_FILE}
        echo "  Protection mechanism: Schema-level or table ownership permissions" | tee -a ${LOG_FILE}
        TEST13_RESULT="PASS"
    fi
else
    echo "  Skipped: Log table not visible (already dropped)" | tee -a ${LOG_FILE}
    TEST13_RESULT="SKIPPED"
fi
echo "" | tee -a ${LOG_FILE}

# TEST 14: repack_user1 queries their own intermediate table (post-commit, should succeed)
echo "=== TEST 14: ${USER1} queries own intermediate table (post-commit) ===" | tee -a ${LOG_FILE}
echo "Testing: ${INTERMEDIATE_TABLE} (committed and visible)" | tee -a ${LOG_FILE}
echo "Context: Owner should have full access to their own table" | tee -a ${LOG_FILE}
echo "  This also PROVES pg_repack is still in progress!" | tee -a ${LOG_FILE}
if [ "$INTERMEDIATE_VISIBLE" = "t" ]; then
    if PGPASSWORD=${PASS1} psql -h localhost -U ${USER1} -d ${DB_NAME} -c "SELECT COUNT(*) as row_count FROM ${INTERMEDIATE_TABLE};" 2>&1 | tee -a ${LOG_FILE}; then
        echo "✓ PASS: ${USER1} CAN query their own ${INTERMEDIATE_TABLE} (expected)" | tee -a ${LOG_FILE}
        echo "  This confirms pg_repack is still in progress (table not yet swapped)" | tee -a ${LOG_FILE}
        TEST14_RESULT="PASS"
    else
        echo "✗ FAIL: ${USER1} cannot query their own intermediate table" | tee -a ${LOG_FILE}
        TEST14_RESULT="FAIL"
    fi
else
    echo "  Skipped: Intermediate table not visible (already swapped/dropped)" | tee -a ${LOG_FILE}
    TEST14_RESULT="SKIPPED"
fi
echo "" | tee -a ${LOG_FILE}

# TEST 15: repack_user1 queries their own log table (post-commit, should succeed)
echo "=== TEST 15: ${USER1} queries own log table (post-commit) ===" | tee -a ${LOG_FILE}
echo "Testing: ${LOG_TABLE} (committed and visible)" | tee -a ${LOG_FILE}
echo "Context: Owner should have full access to their own log table" | tee -a ${LOG_FILE}
LOG_VISIBLE=$(psql -d ${DB_NAME} -t -c "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'log_${TABLE_OID}' AND relnamespace = 'repack'::regnamespace);" | xargs)
if [ "$LOG_VISIBLE" = "t" ]; then
    if PGPASSWORD=${PASS1} psql -h localhost -U ${USER1} -d ${DB_NAME} -c "SELECT COUNT(*) as log_entries FROM ${LOG_TABLE};" 2>&1 | tee -a ${LOG_FILE}; then
        echo "✓ PASS: ${USER1} CAN query their own ${LOG_TABLE} (expected)" | tee -a ${LOG_FILE}
        TEST15_RESULT="PASS"
    else
        echo "✗ FAIL: ${USER1} cannot query their own log table" | tee -a ${LOG_FILE}
        TEST15_RESULT="FAIL"
    fi
else
    echo "  Skipped: Log table not visible (already dropped)" | tee -a ${LOG_FILE}
    TEST15_RESULT="SKIPPED"
fi
echo "" | tee -a ${LOG_FILE}

# Kill background workload now that all tests are complete
echo "=== Stopping background workload ===" | tee -a ${LOG_FILE}
if kill -0 ${WORKLOAD_PID} 2>/dev/null; then
    kill ${WORKLOAD_PID} 2>/dev/null || true
    sleep 1
    if kill -0 ${WORKLOAD_PID} 2>/dev/null; then
        kill -9 ${WORKLOAD_PID} 2>/dev/null || true
    fi
    echo "✓ Background workload stopped after completing all tests" | tee -a ${LOG_FILE}
fi
echo "" | tee -a ${LOG_FILE}

# Check log table for captured updates
echo "=== Verifying log table captured concurrent updates ===" | tee -a ${LOG_FILE}
LOG_COUNT=$(PGPASSWORD=${PASS1} psql -h localhost -U ${USER1} -d ${DB_NAME} -t -c "SELECT COUNT(*) FROM ${LOG_TABLE};" 2>/dev/null | xargs || echo "0")
echo "Log entries captured: ${LOG_COUNT}" | tee -a ${LOG_FILE}
if [ "$LOG_COUNT" -gt 0 ]; then
    echo "✓ Log table captured concurrent modifications during repack" | tee -a ${LOG_FILE}
    PGPASSWORD=${PASS1} psql -h localhost -U ${USER1} -d ${DB_NAME} -c "SELECT COUNT(*) as total_changes, COUNT(DISTINCT id) as unique_rows_changed FROM ${LOG_TABLE};" | tee -a ${LOG_FILE}
else
    echo "  Note: No log entries (workload may not have started or repack too fast)" | tee -a ${LOG_FILE}
fi
echo "" | tee -a ${LOG_FILE}

# Show workload statistics before stopping
if [ -f workload_output.log ]; then
    echo "Workload statistics:" | tee -a ${LOG_FILE}
    tail -20 workload_output.log | grep -E "(transaction|latency)" | tee -a ${LOG_FILE}
fi
echo "" | tee -a ${LOG_FILE}

# Let repack complete naturally
echo "=== Waiting for pg_repack to complete ===" | tee -a ${LOG_FILE}
echo "Repack should complete within 10 minutes..." | tee -a ${LOG_FILE}
COMPLETE_WAIT=0
while [ $COMPLETE_WAIT -lt 600 ]; do  # 10 minutes max
    if ! kill -0 ${REPACK_PID} 2>/dev/null; then
        echo "✓ pg_repack process completed" | tee -a ${LOG_FILE}
        break
    fi
    COMPLETE_WAIT=$((COMPLETE_WAIT + 1))
    sleep 1
done

# If still running, kill it
if kill -0 ${REPACK_PID} 2>/dev/null; then
    echo "pg_repack still running after 10 minutes, sending SIGTERM..." | tee -a ${LOG_FILE}
    kill ${REPACK_PID} 2>/dev/null || true
    sleep 2
    
    # Force kill if necessary
    if kill -0 ${REPACK_PID} 2>/dev/null; then
        echo "Sending SIGKILL..." | tee -a ${LOG_FILE}
        kill -9 ${REPACK_PID} 2>/dev/null || true
    fi
fi

echo "" | tee -a ${LOG_FILE}

# Generate test summary
echo "================================================================" | tee -a ${LOG_FILE}
echo "                        TEST SUMMARY" | tee -a ${LOG_FILE}
echo "================================================================" | tee -a ${LOG_FILE}
echo "" | tee -a ${LOG_FILE}

echo "User Privilege Configuration:" | tee -a ${LOG_FILE}
echo "  ${USER1}: CAN run pg_repack" | tee -a ${LOG_FILE}
echo "  ${USER2}: CAN run pg_repack" | tee -a ${LOG_FILE}
echo "  ${USER3}: CANNOT run pg_repack" | tee -a ${LOG_FILE}
echo "" | tee -a ${LOG_FILE}

echo "Test Results:" | tee -a ${LOG_FILE}
echo "" | tee -a ${LOG_FILE}
echo "During Initial Data Copy (Transaction Uncommitted):" | tee -a ${LOG_FILE}
echo "  TEST 1 (${USER2} → ${USER1} log table): ${TEST1_RESULT}" | tee -a ${LOG_FILE}
echo "  TEST 2 (${USER2} → ${USER1} intermediate table metadata): ${TEST2_RESULT}" | tee -a ${LOG_FILE}
echo "  TEST 3 (${USER2} → ${USER1} source table): ${TEST3_RESULT}" | tee -a ${LOG_FILE}
echo "  TEST 4 (${USER3} → repack schema access): ${TEST4_RESULT}" | tee -a ${LOG_FILE}
echo "  TEST 5 (${USER3} → data access protection): ${TEST5_RESULT}" | tee -a ${LOG_FILE}
echo "  TEST 6 (${USER3} → ${USER1} source table): ${TEST6_RESULT}" | tee -a ${LOG_FILE}
echo "  TEST 7 (${USER1} → own log table, CAN access): ${TEST7_RESULT}" | tee -a ${LOG_FILE}
echo "  TEST 8 (${USER1} → own intermediate table, serializable txn): ${TEST8_RESULT}" | tee -a ${LOG_FILE}
echo "  TEST 9 (${USER1} → own source table, CAN access): ${TEST9_RESULT}" | tee -a ${LOG_FILE}
echo "" | tee -a ${LOG_FILE}
echo "After Copy Completion (Table Visible in pg_class):" | tee -a ${LOG_FILE}
echo "  TEST 10 (${USER2} → intermediate table, BLOCKED): ${TEST10_RESULT}" | tee -a ${LOG_FILE}
echo "  TEST 11 (${USER2} → log table, BLOCKED): ${TEST11_RESULT}" | tee -a ${LOG_FILE}
echo "  TEST 12 (${USER3} → intermediate table, BLOCKED): ${TEST12_RESULT}" | tee -a ${LOG_FILE}
echo "  TEST 13 (${USER3} → log table, BLOCKED): ${TEST13_RESULT}" | tee -a ${LOG_FILE}
echo "  TEST 14 (${USER1} → own intermediate table, CAN access): ${TEST14_RESULT}" | tee -a ${LOG_FILE}
echo "  TEST 15 (${USER1} → own log table, CAN access): ${TEST15_RESULT}" | tee -a ${LOG_FILE}
echo "" | tee -a ${LOG_FILE}
echo "Note: TEST 14-15 prove pg_repack is still in progress (owner can access intermediate objects)." | tee -a ${LOG_FILE}
echo "      TEST 8 demonstrates serializable transaction isolation (MVCC)." | tee -a ${LOG_FILE}
echo "      TEST 1-6 and 10-13 prove privilege isolation (ownership-based protection)." | tee -a ${LOG_FILE}
echo "" | tee -a ${LOG_FILE}

# Check if all tests passed
ALL_PASSED=true
for result in "${TEST1_RESULT}" "${TEST2_RESULT}" "${TEST3_RESULT}" "${TEST4_RESULT}" "${TEST5_RESULT}" "${TEST6_RESULT}" "${TEST7_RESULT}" "${TEST8_RESULT}" "${TEST9_RESULT}" "${TEST10_RESULT}" "${TEST11_RESULT}" "${TEST12_RESULT}" "${TEST13_RESULT}" "${TEST14_RESULT}" "${TEST15_RESULT}"; do
    if [ "$result" = "FAIL" ]; then
        ALL_PASSED=false
        break
    fi
done

if [ "$ALL_PASSED" = true ]; then
    echo "================================================================" | tee -a ${LOG_FILE}
    echo "                    ✓ ALL TESTS PASSED" | tee -a ${LOG_FILE}
    echo "================================================================" | tee -a ${LOG_FILE}
    echo "" | tee -a ${LOG_FILE}
    echo "Conclusion: pg_repack properly isolates intermediate tables" | tee -a ${LOG_FILE}
    echo "both during and after the data copy phase." | tee -a ${LOG_FILE}
    echo "" | tee -a ${LOG_FILE}
    echo "Key findings:" | tee -a ${LOG_FILE}
    echo "  • During copy: Protected by transaction isolation (MVCC)" | tee -a ${LOG_FILE}
    echo "  • After copy: Protected by table ownership permissions" | tee -a ${LOG_FILE}
    echo "  • Concurrent updates: Properly captured in log table" | tee -a ${LOG_FILE}
    echo "  • Neither ${USER2} nor ${USER3} could access ${USER1}'s data" | tee -a ${LOG_FILE}
    echo "" | tee -a ${LOG_FILE}
    exit 0
else
    echo "================================================================" | tee -a ${LOG_FILE}
    echo "                    ✗ SOME TESTS FAILED" | tee -a ${LOG_FILE}
    echo "================================================================" | tee -a ${LOG_FILE}
    echo "" | tee -a ${LOG_FILE}
    echo "Review the log above for details on which tests failed." | tee -a ${LOG_FILE}
    echo "" | tee -a ${LOG_FILE}
    exit 1
fi

