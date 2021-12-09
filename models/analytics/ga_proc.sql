{{ config(materialized='table') }}

SELECT 
	tra.bigquery_name,
	tra.lookup_platform,
	tra.test_id,
	tra.variant_id,
	tra.variant_name,
	tra.creative_url,
	coalesce(tra.date,trans.date) as date,
	coalesce(tra.sourcemedium,trans.sourcemedium) as sourcemedium,
	coalesce(tra.devicecategory,trans.devicecategory) as devicecategory,
	tra.sessions sessions,
	tra.bounces bounces,
	trans.revenue revenue,
	trans.itemquantity as itemquantity,
	trans.transactions as transactions,
	tra.productaddstocart as productaddstocart,
	addtocart,
	carttopurchase
FROM {{ref('ga_traffic_proc')}} tra
left join {{ref('ga_transactions_proc')}} as trans
on  tra.bigquery_name = trans.bigquery_name 
and tra.lookup_platform = trans.lookup_platform 
and trans.date = tra.date 
and trans.devicecategory = tra.devicecategory 
and trans.sourcemedium = tra.sourcemedium

        

