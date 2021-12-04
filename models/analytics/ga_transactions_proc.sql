-- depends_on: {{ ref('ga_conversions') }}

{{ config(materialized='table') }}

{% set accounts = get_column_values(table=ref('accounts_proc'), column='bigquery_name', max_records=50, filter_column='platform', filter_value='Google Analytics') %}

{% if accounts != '' %}

with ga_report as (

	    {% for account in accounts %}
		
		   	SELECT
		   	'{{account}}' as bigquery_name,
		   	'Google Analytics' as lookup_platform,
			ta.ga_date as date,
			ta.ga_sourcemedium as sourcemedium,
			ta.ga_devicecategory as devicecategory,
			ta.ga_transactionrevenue revenue,
			ta.ga_itemquantity as itemquantity,
			ta.ga_transactions as transactions,
			ta._sdc_sequence,
			first_value(ta._sdc_sequence) OVER (PARTITION BY ta.ga_hostname, ta.ga_landingpagepath, ta.ga_date, ta.ga_sourcemedium ORDER BY ta._sdc_sequence DESC) lv
			FROM `{{ target.project }}.ga_{{account}}.transactions_report` as ta
		    {% if not loop.last %} UNION ALL {% endif %}
	   {% endfor %}

)


SELECT 
bigquery_name,
lookup_platform,
date,
sourcemedium,
devicecategory,
sum(revenue) revenue,
sum(itemquantity) itemquantity,
sum(transactions) transactions
FROM ga_report
where lv = _sdc_sequence
group by bigquery_name, lookup_platform, date ,sourcemedium,devicecategory

{% endif %}