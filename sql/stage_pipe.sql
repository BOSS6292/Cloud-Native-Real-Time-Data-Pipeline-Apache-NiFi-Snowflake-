CREATE OR REPLACE STAGE SCD_DEMO.SCD2.customer_ext_stage
    url='s3://snowflake-dw-tejas/stream_data/'
    credentials=(aws_key_id='ENTER YOUR AWS KEY' aws_secret_key='ENTER YOUR AWS SECRET KEY');

CREATE OR REPLACE FILE FORMAT SCD_DEMO.SCD2.CSV
TYPE = CSV,
FIELD_DELIMITER = ","
SKIP_HEADER = 1;

LIST @SCD_DEMO.SCD2.customer_ext_stage;

CREATE OR REPLACE PIPE customer_s3_pipe
  auto_ingest = true
  AS
  COPY INTO customer_raw
  FROM @customer_ext_stage
  FILE_FORMAT = CSV;

show pipes;
