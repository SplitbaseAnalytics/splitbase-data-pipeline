{{ config(materialized='table') }}

with control as(
SELECT   date
        ,bigquery_name
        ,lookup_platform
        ,sum(sessions) as control_session 
        ,round(sum(revenue),2) as control_revenue 
        ,sum(transactions) as control_order
        ,sum(itemquantity) as control_units
        ,sum(bounces) as control_bounces
        ,sum(productaddstocart) as control_cartaddvisits
FROM {{ref('ga_proc')}} 
where variant_name='Control'
group by  1,2,3
order by  1,2,3
)
, control_calculation as 
(
    select 
        case when (c.control_order         =0 or c.control_session      =0) then 0 else round((c.control_order          / c.control_session )*100 ,1)       end as control_conv
       ,case when (c.control_cartaddvisits =0 or c.control_session      =0) then 0 else round((c.control_cartaddvisits  / c.control_session )*100,1)        end as control_add_to_cart
       ,case when (c.control_order         =0 or c.control_cartaddvisits=0) then 0 else round((c.control_order          / c.control_cartaddvisits)*100,1)   end as control_complete_checkout
       ,case when (c.control_bounces       =0 or c.control_session      =0) then 0 else round((c.control_bounces        / c.control_session)*100,1)         end as control_bouncerate
       ,date
       ,bigquery_name
       ,lookup_platform
    from control  as c
)
,Variant1 as
(
SELECT   date
        ,bigquery_name
        ,lookup_platform
        ,sum(coalesce(sessions,0)) as variant1_session 
        ,round(sum(coalesce(revenue,0)),2) as variant1_revenue 
        ,sum(coalesce(transactions,0)) as variant1_order
        ,sum(coalesce(itemquantity,0)) as variant1_units
        ,sum(coalesce(bounces,0)) as variant1_bounces
        ,sum(coalesce(productaddstocart,0)) as variant1_cartaddvisits
FROM {{ref('ga_proc')}}
where variant_name='Variant 1'
group by  1,2,3
order by  1,2,3
)
, variant1_calculation as 
(
    select 
        case when (c.variant1_order         =0 or c.variant1_session            =0) then 0 else round((c.variant1_order          / c.variant1_session)*100 ,1)      end as variant1_conv
       ,case when (c.variant1_cartaddvisits =0 or c.variant1_session            =0) then 0 else round((c.variant1_cartaddvisits  / c.variant1_session )*100,1)      end as variant1_add_to_cart
       ,case when (c.variant1_order         =0 or c.variant1_cartaddvisits      =0) then 0 else round((c.variant1_order          / c.variant1_cartaddvisits)*100,1) end as variant1_complete_checkout
       ,case when (c.variant1_bounces       =0 or c.variant1_session            =0) then 0 else round((c.variant1_bounces        / c.variant1_session)*100,1)       end as variant1_bouncerate
       ,date
       ,bigquery_name
       ,lookup_platform
    from Variant1  as c
)

,Variant2 as
(
SELECT   date
        ,bigquery_name
        ,lookup_platform
        ,sum(coalesce(sessions,0)) as variant2_session 
        ,round(sum(coalesce(revenue,0)),2) as variant2_revenue 
        ,sum(coalesce(transactions,0)) as variant2_order
        ,sum(coalesce(itemquantity,0)) as variant2_units
        ,sum(coalesce(bounces,0)) as variant2_bounces
        ,sum(coalesce(productaddstocart,0)) as variant2_cartaddvisits
FROM {{ref('ga_proc')}} 
where variant_name='Variant 2'
group by  1,2,3
order by  1,2,3
)
, variant2_calculation as 
(
    select 
        case when (c.variant2_order          =0 or c.variant2_session            =0) then 0 else round((c.variant2_order          / c.variant2_session)*100 ,1)      end as variant2_conv
       ,case when (c.variant2_cartaddvisits  =0 or c.variant2_session            =0) then 0 else round((c.variant2_cartaddvisits  / c.variant2_session )*100,1)      end as variant2_add_to_cart
       ,case when (c.variant2_order          =0 or c.variant2_cartaddvisits      =0) then 0 else round((c.variant2_order          / c.variant2_cartaddvisits)*100,1) end as variant2_complete_checkout
       ,case when (c.variant2_bounces        =0 or c.variant2_session            =0) then 0 else round((c.variant2_bounces        / c.variant2_session)*100,1)       end as variant2_bouncerate
       ,date
       ,bigquery_name
       ,lookup_platform
    from Variant2  as c
)


    select
        c.*
       ,cc.control_conv
       ,cc.control_add_to_cart
       ,cc.control_complete_checkout
       ,cc.control_bouncerate	
       ,v1.variant1_session
       ,v1.variant1_revenue
       ,v1.variant1_order
       ,v1.variant1_units
       ,v1.variant1_bounces
       ,v1.variant1_cartaddvisits
       ,vc1.variant1_Conv
       ,vc1.variant1_add_to_Cart 
       ,vc1.variant1_complete_checkout 
       ,vc1.variant1_bouncerate	
       ,v2.variant2_session
       ,v2.variant2_revenue
       ,v2.variant2_order
       ,v2.variant2_units
       ,v2.variant2_bounces
       ,v2.variant2_cartaddvisits
       ,vc2.variant2_Conv 
       ,vc2.variant2_add_to_cart 
       ,vc2.variant2_complete_checkout 
       ,vc2.variant2_bouncerate	
    from control as c
    left join Variant1 as v1 
    on c.date = v1.date and c.bigquery_name= v1.bigquery_name and c.lookup_platform=v1.lookup_platform
    left join variant1_calculation as vc1
    on vc1.date = v1.date and vc1.bigquery_name = v1.bigquery_name and vc1.lookup_platform = v1.lookup_platform
    left join Variant2 as v2 
    on c.date = v2.date and c.bigquery_name= v2.bigquery_name and c.lookup_platform=v2.lookup_platform
    left join variant2_calculation  as vc2
    on vc2.date = c.date and vc2.bigquery_name = c.bigquery_name and vc2.lookup_platform = c.lookup_platform
    left join control_calculation  as cc 
    on cc.date = c.date and cc.bigquery_name = c.bigquery_name and cc.lookup_platform = c.lookup_platform
    order by date


