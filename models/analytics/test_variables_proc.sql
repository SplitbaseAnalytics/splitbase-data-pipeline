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
FROM  ( 

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
        time_of_entry,
        first_value(time_of_entry) OVER (PARTITION BY site ORDER BY time_of_entry DESC) lv
        FROM `{{ target.project }}.agency_data_pipeline.test_variables` 

) 

WHERE lv = time_of_entry
group by site, platform, dimension_index, account, bigquery_name, dimension_index, variant_id, variant_name, test_id, creative_url, test_start_date, test_end_date, time_of_entry