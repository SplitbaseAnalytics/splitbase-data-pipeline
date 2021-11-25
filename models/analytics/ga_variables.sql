SELECT
site,
platform,
dimension_index,
variant_id,
variant_name,
test_id,
creative_url,
test_start_date,
test_end_date,
account,
bigquery_name,
time_of_entry
FROM {{ ref('test_variables_proc') }}
WHERE platform = 'Google Analytics'