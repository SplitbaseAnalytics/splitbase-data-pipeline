SELECT
    site,
    account,
    lower(REGEXP_REPLACE(account, '[^a-zA-Z0-9]+', '')) AS account_normalized,
    platform,
    bigquery_name
FROM 
(
    SELECT
        site,
        account,
        platform,
        bigquery_name,
        time_of_entry,
        first_value(time_of_entry) over (partition by site order by time_of_entry desc) lv
  FROM `{{ target.project }}.agency_data_pipeline.accounts`
)
  WHERE time_of_entry = lv