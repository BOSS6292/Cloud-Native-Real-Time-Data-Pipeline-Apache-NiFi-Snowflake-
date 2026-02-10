-- =====================================================
-- Stored Procedure + Task Automation
-- File: scd_raw_task.sql
-- Purpose: Merge raw customer data into base table and automate execution
-- =====================================================

-- =====================================================
-- Stored Procedure: Merge Raw Data
-- =====================================================
CREATE OR REPLACE PROCEDURE pdr_scd_demo()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
var merge_cmd = `
MERGE INTO customer c
USING customer_raw cr
ON c.customer_id = cr.customer_id

WHEN MATCHED AND
(
c.customer_id <> cr.customer_id OR
c.first_name  <> cr.first_name  OR
c.last_name   <> cr.last_name   OR
c.email       <> cr.email       OR
c.street      <> cr.street      OR
c.city        <> cr.city        OR
c.state       <> cr.state       OR
c.country     <> cr.country
)
THEN UPDATE SET
c.customer_id = cr.customer_id,
c.first_name  = cr.first_name,
c.last_name   = cr.last_name,
c.email       = cr.email,
c.street      = cr.street,
c.city        = cr.city,
c.state       = cr.state,
c.country     = cr.country,
update_timestamp = CURRENT_TIMESTAMP();

WHEN NOT MATCHED THEN
INSERT (customer_id, first_name, last_name, email, street, city, state, country)
VALUES (cr.customer_id, cr.first_name, cr.last_name, cr.email, cr.street, cr.city, cr.state, cr.country);
`;

var truncate_cmd = `TRUNCATE TABLE SCD_DEMO.SCD2.customer_raw;`;

snowflake.createStatement({sqlText: merge_cmd}).execute();
snowflake.createStatement({sqlText: truncate_cmd}).execute();

return 'Merge and truncate completed';
$$;

-- =====================================================
-- Role Setup (Optional for Task Execution)
-- =====================================================
USE ROLE SECURITYADMIN;
CREATE OR REPLACE ROLE taskadmin;

USE ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE taskadmin;

USE ROLE SECURITYADMIN;
GRANT ROLE taskadmin TO ROLE sysadmin;

-- =====================================================
-- Task Creation
-- =====================================================
CREATE OR REPLACE TASK tsk_scd_raw
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 minute'
ERROR_ON_NONDETERMINISTIC_MERGE = FALSE
AS
CALL pdr_scd_demo();

-- =====================================================
-- Task Control
-- =====================================================
ALTER TASK tsk_scd_raw RESUME;
-- ALTER TASK tsk_scd_raw SUSPEND;

SHOW TASKS;
