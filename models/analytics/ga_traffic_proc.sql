-- depends_on: {{ ref('ga_conversions') }}
-- depends_on: {{ ref('ga_variables') }}

{{ config(materialized='table') }}

{% set accounts = get_column_values(table=ref('accounts_proc'), column='bigquery_name', max_records=50, filter_column='platform', filter_value='Google Analytics') %}

{% if accounts != '' %}

with ga_report as (

	    {% for account in accounts %}

	    	{% set atc = get_column_values(table=ref('ga_conversions'), column='goal_name', max_records=50, filter_column='goal_type', filter_value='Add to Cart', filter_column_2='bigquery_name', filter_value_2=account ) %}
	    	{% set ctp = get_column_values(table=ref('ga_conversions'), column='goal_name', max_records=50, filter_column='goal_type', filter_value='Cart to Purchase', filter_column_2='bigquery_name', filter_value_2=account ) %}
            {% set ind = get_column_values(table=ref('ga_variables'), column='dimension_index', max_records=50, filter_column='bigquery_name', filter_value=account ) %}

		   	SELECT
		   	'{{account}}' as bigquery_name,
		   	'Google Analytics' as lookup_platform,
			--ga_variable
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
			tr.ga_date as date,
			tr.ga_sourcemedium as sourcemedium,
			tr.ga_devicecategory as devicecategory,
			tr.ga_sessions sessions,
			tr.ga_bounces bounces,
			tr.ga_productaddstocart as productaddstocart,
			{% if atc != [] %}
				{% for goal in atc %}
					cast(ga_goal{{goal}}completions as int64) 
					{% if not loop.last %} + {% endif %} 
					{% if loop.last %} as addtocart, {% endif %} 
				{% endfor %}
			{% else %}				
				null as addtocart,		
			{% endif %}
			{% if ctp != [] %}
				{% for goal in ctp %}
					cast(ga_goal{{goal}}completions as int64) 
					{% if not loop.last %} + {% endif %} 
					{% if loop.last %} as carttopurchase, {% endif %} 
				{% endfor %}
			{% else %}				
				null as carttopurchase,		
			{% endif %}
			tr._sdc_sequence,
			first_value(tr._sdc_sequence) OVER (PARTITION BY tr.ga_hostname, tr.ga_landingpagepath, tr.ga_date, tr.ga_sourcemedium ORDER BY tr._sdc_sequence DESC) lv
			FROM {{ref('ga_variables')}} v
            inner join `{{ target.project }}.ga_{{account}}.traffic_report` as tr 
            on lower(REGEXP_REPLACE(tr.web_property_id, '[^a-zA-Z0-9]+', ''))  = lower(REGEXP_REPLACE(v.account, '[^a-zA-Z0-9]+', ''))
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
sum(sessions) sessions,
sum(bounces) bounces,
sum(productaddstocart) productaddstocart,
sum(addtocart) addtocart,
sum(carttopurchase) carttopurchase,
FROM ga_report
where lv = _sdc_sequence
group by bigquery_name, lookup_platform, date, test_id, variant_id, variant_name , creative_url ,sourcemedium,devicecategory

{% endif %}