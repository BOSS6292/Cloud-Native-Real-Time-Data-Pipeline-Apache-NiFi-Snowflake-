-- =====================================================
-- Stream-Based SCD Type 2 Processing
-- =====================================================

-- Optional: Verify stream exists
SHOW STREAMS;

-- =====================================================
-- View: Determines changes captured by Stream
-- =====================================================
CREATE OR REPLACE VIEW v_customer_change_data AS

-- Handle INSERT operations
SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY,
start_time, end_time, is_current, 'I' AS dml_type
FROM (
SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY,
update_timestamp AS start_time,
LAG(update_timestamp) OVER (PARTITION BY customer_id ORDER BY update_timestamp DESC) AS end_time_raw,
CASE WHEN end_time_raw IS NULL THEN '9999-12-31'::timestamp_ntz ELSE end_time_raw END AS end_time,
CASE WHEN end_time_raw IS NULL THEN TRUE ELSE FALSE END AS is_current
FROM (
SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY, UPDATE_TIMESTAMP
FROM SCD_DEMO.SCD2.customer_table_changes
WHERE METADATA$ACTION = 'INSERT'
AND METADATA$ISUPDATE = 'FALSE'
)
)

UNION

-- Handle UPDATE operations
SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY,
start_time, end_time, is_current, dml_type
FROM (
SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY,
update_timestamp AS start_time,
LAG(update_timestamp) OVER (PARTITION BY customer_id ORDER BY update_timestamp DESC) AS end_time_raw,
CASE WHEN end_time_raw IS NULL THEN '9999-12-31'::timestamp_ntz ELSE end_time_raw END AS end_time,
CASE WHEN end_time_raw IS NULL THEN TRUE ELSE FALSE END AS is_current,
dml_type
FROM (
-- Insert new version after update
SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY,
update_timestamp, 'I' AS dml_type
FROM customer_table_changes
WHERE METADATA$ACTION = 'INSERT'
AND METADATA$ISUPDATE = 'TRUE'

```
    UNION

    -- Mark previous version as outdated
    SELECT CUSTOMER_ID, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
           start_time, 'U' AS dml_type
    FROM customer_history
    WHERE customer_id IN (
        SELECT DISTINCT customer_id
        FROM customer_table_changes
        WHERE METADATA$ACTION = 'DELETE'
          AND METADATA$ISUPDATE = 'TRUE'
    )
      AND is_current = TRUE
)
```

)

UNION

-- Handle DELETE operations
SELECT ctc.CUSTOMER_ID,
NULL, NULL, NULL, NULL, NULL, NULL, NULL,
ch.start_time,
CURRENT_TIMESTAMP()::timestamp_ntz,
NULL,
'D'
FROM customer_history ch
JOIN customer_table_changes ctc
ON ch.customer_id = ctc.customer_id
WHERE ctc.METADATA$ACTION = 'DELETE'
AND ctc.METADATA$ISUPDATE = 'FALSE'
AND ch.is_current = TRUE;

-- =====================================================
-- Task: Apply changes to history table
-- =====================================================
CREATE OR REPLACE TASK tsk_scd_hist
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 minute'
ERROR_ON_NONDETERMINISTIC_MERGE = FALSE
AS

MERGE INTO customer_history ch
USING v_customer_change_data ccd
ON ch.CUSTOMER_ID = ccd.CUSTOMER_ID
AND ch.start_time = ccd.start_time

WHEN MATCHED AND ccd.dml_type = 'U' THEN
UPDATE SET
ch.end_time = ccd.end_time,
ch.is_current = FALSE

WHEN MATCHED AND ccd.dml_type = 'D' THEN
UPDATE SET
ch.end_time = ccd.end_time,
ch.is_current = FALSE

WHEN NOT MATCHED AND ccd.dml_type = 'I' THEN
INSERT (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY,
start_time, end_time, is_current)
VALUES (ccd.CUSTOMER_ID, ccd.FIRST_NAME, ccd.LAST_NAME, ccd.EMAIL,
ccd.STREET, ccd.CITY, ccd.STATE, ccd.COUNTRY,
ccd.start_time, ccd.end_time, ccd.is_current);

-- =====================================================
-- Task Control
-- =====================================================
ALTER TASK tsk_scd_hist RESUME;
-- ALTER TASK tsk_scd_hist SUSPEND;
