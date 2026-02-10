-- =====================================================
-- Stream Testing Examples
-- File: stream_test_examples.sql
-- Purpose: Generate DML activity to validate Stream + Task behavior
-- =====================================================

---

-- Insert Test Record

---

INSERT INTO customer VALUES (
223136,
'Jessica',
'Arnold',
'[tanner39@smith.com](mailto:tanner39@smith.com)',
'595 Benjamin Forge Suite 124',
'Michaelstad',
'Connecticut',
'Cape Verde',
CURRENT_TIMESTAMP()
);

---

-- Update Test Record

---

UPDATE customer
SET FIRST_NAME = 'Jessica',
update_timestamp = CURRENT_TIMESTAMP()::timestamp_ntz
WHERE customer_id = 72;

---

-- Delete Test Record

---

DELETE FROM customer
WHERE customer_id = 73;

---

-- Additional Variation Tests

---

INSERT INTO customer VALUES (
223137,
'Test',
'User',
'[test.user@email.com](mailto:test.user@email.com)',
'123 Demo Street',
'SampleCity',
'Texas',
'USA',
CURRENT_TIMESTAMP()
);

UPDATE customer
SET FIRST_NAME = 'UpdatedName'
WHERE customer_id = 223137;

DELETE FROM customer
WHERE customer_id = 136
AND FIRST_NAME = 'Kim';

---

-- Validation Queries

---

-- Check stream output
SELECT * FROM customer_table_changes;

-- Check current table state
SELECT * FROM customer
WHERE customer_id IN (72, 73, 223136, 223137);

-- Check SCD history
SELECT *
FROM customer_history
WHERE customer_id IN (72, 73, 223136, 223137);

SELECT *
FROM customer_history
WHERE is_current = TRUE;

SELECT *
FROM customer_history
WHERE is_current = FALSE;

-- Optional: Check for duplicate customer IDs
SELECT COUNT(*), customer_id
FROM customer
GROUP BY customer_id
HAVING COUNT(*) = 1;
