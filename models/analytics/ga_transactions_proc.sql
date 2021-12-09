-- depends_on: {{ ref('ga_conversions') }}
-- depends_on: {{ ref('ga_variables') }}

{{ config(materialized='table') }}

{% set accounts = get_column_values(table=ref('accounts_proc'), column='bigquery_name', max_records=50, filter_column='platform', filter_value='Google Analytics') %}

{% if accounts != '' %}

with ga_report as (

	    {% for account in accounts %}

		    {% set ind = get_column_values(table=ref('ga_variables'), column='dimension_index', max_records=50, filter_column='bigquery_name', filter_value=account ) %}

		   	SELECT
		   	'{{account}}' as bigquery_name,
		   	'Google Analytics' as lookup_platform,
			test_id,
			{% if ind != [] %}
				{% for goal in ind %}
					cast(ga_dimension{{goal}} as string) 
					{% if not loop.last %} + {% endif %} 
					{% if loop.last %} as variant_id, {% endif %} 
				{% endfor %}
			{% else %}				
				null as variant_id,
			{% endif %}
			variant_name,
			creative_url,
			ta.ga_date as date,
			ta.ga_sourcemedium as sourcemedium,
			ta.ga_devicecategory as devicecategory,
			ta.ga_transactionrevenue revenue,
			ta.ga_itemquantity as itemquantity,
			ta.ga_transactions as transactions,
			--ta._sdc_sequence,
			--first_value(ta._sdc_sequence) OVER (PARTITION BY ta.ga_hostname, ta.ga_landingpagepath, ta.ga_date, ta.ga_sourcemedium , ta.ga_devicecategory ORDER BY ta._sdc_sequence DESC) lv
			FROM {{ref('ga_variables')}} v
			inner join  `{{ target.project }}.ga_{{account}}.transactions_report` as ta
			on lower(REGEXP_REPLACE(ta.web_property_id, '[^a-zA-Z0-9]+', ''))  = lower(REGEXP_REPLACE(v.account, '[^a-zA-Z0-9]+', '')) --and v.variant_id = ga_dimension1
			and {% if ind != [] %}
				{% for goal in ind %}
					cast(ga_dimension{{goal}} as string) = v.variant_id 
					{% if loop.last %}{% endif %} 
				{% endfor %}
			{% else %}				
				1=1,
			{% endif %}
		    {% if not loop.last %} UNION ALL {% endif %}
	   {% endfor %}

)


SELECT 
bigquery_name,
lookup_platform,
test_id,
variant_id,
variant_name,
creative_url,
date,
sourcemedium,
devicecategory,
sum(revenue) revenue,
sum(itemquantity) itemquantity,
sum(transactions) transactions
FROM ga_report
--where lv = _sdc_sequence
group by bigquery_name, lookup_platform, date, test_id, variant_id, variant_name , creative_url ,sourcemedium,devicecategory

{% endif %}