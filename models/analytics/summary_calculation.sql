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
       ,case when (c.control_revenue       =0 or c.control_session      =0) then 0 else round(c.control_revenue         / c.control_session,2)              end as control_RPS
       ,case when (c.control_revenue       =0 or c.control_order        =0) then 0 else round(c.control_revenue         / c.control_order,2)                end as control_AOV
       ,case when (c.control_units         =0 or c.control_order        =0) then 0 else round(c.control_units           / c.control_order,2)                end as control_UPT
       ,case when (c.control_revenue       =0 or c.control_units        =0) then 0 else round(c.control_revenue         / c.control_units,2)                end as control_AUR
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
       ,case when (c.variant1_revenue       =0 or c.variant1_session            =0) then 0 else round(c.variant1_revenue         / c.variant1_session,2)            end as variant1_RPS
       ,case when (c.variant1_revenue       =0 or c.variant1_order              =0) then 0 else round(c.variant1_revenue         / c.variant1_order,2)              end as variant1_AOV
       ,case when (c.variant1_units         =0 or c.variant1_order              =0) then 0 else round(c.variant1_units           / c.variant1_order,2)              end as variant1_UPT
       ,case when (c.variant1_revenue       =0 or c.variant1_units              =0) then 0 else round(c.variant1_revenue         / c.variant1_units,2)              end as variant1_AUR
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
       ,case when (c.variant2_revenue        =0 or c.variant2_session            =0) then 0 else round(c.variant2_revenue         / c.variant2_session,2)            end as variant2_RPS
       ,case when (c.variant2_revenue        =0 or c.variant2_order              =0) then 0 else round(c.variant2_revenue         / c.variant2_order,2)              end as variant2_AOV
       ,case when (c.variant2_units          =0 or c.variant2_order              =0) then 0 else round(c.variant2_units           / c.variant2_order,2)              end as variant2_UPT
       ,case when (c.variant2_revenue        =0 or c.variant2_units              =0) then 0 else round(c.variant2_revenue         / c.variant2_units,2)              end as variant2_AUR
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
       ,cc.control_RPS
       ,cc.control_AOV
       ,cc.control_UPT
       ,cc.control_AUR
       ,cc.control_bouncerate	
       ,cast(c.control_session as int) as v1_normalized_sessions
       ,round(c.control_session  * vc1.variant1_RPS,2) as v1_normalized_revenue
       ,cast(c.control_session  * vc1.variant1_conv as int) as v1_normalized_orders
       ,cast(c.control_session  * vc1.variant1_conv * vc1.variant1_UPT  as int)  as v1_normalized_units
       ,cast(c.control_session  * vc1.variant1_bouncerate as int)  as v1_normalized_bounces
       ,cast(c.control_session  * vc1.variant1_add_to_Cart as int) as  v1_normalized_cart_add_visits
       ,vc1.variant1_Conv v1_normalized_conv
       ,vc1.variant1_add_to_Cart v1_normalized_add_to_Cart
       ,vc1.variant1_complete_checkout v1_normalized_complete_checkout
       ,vc1.variant1_RPS v1_normalized_RPS
       ,vc1.variant1_AOV v1_normalized_AOV
       ,vc1.variant1_UPT v1_normalized_UPT
       ,vc1.variant1_AUR v1_normalized_AUR
       ,vc1.variant1_bouncerate	v1_normalized_bouncerate
       ,cast(c.control_session  as int) as v2_normalized_sessions
       ,round(c.control_session  * vc2.variant2_RPS ,2) as v2_normalized_revenue
       ,cast(c.control_session  * vc2.variant2_conv as int) as v2_normalized_orders
       ,cast(c.control_session  * vc2.variant2_conv * vc2.variant2_UPT as int)  as v2_normalized_Units
       ,cast(c.control_session  * vc2.variant2_bouncerate  as int) as v2_normalized_bounces
       ,cast(c.control_session  * vc2.variant2_add_to_Cart as int) as v2_normalized_cart_add_visits
       ,vc2.variant2_Conv v2_normalized_conv
       ,vc2.variant2_add_to_cart v2_normalized_add_to_cart
       ,vc2.variant2_complete_checkout v2_normalized_complete_checkout
       ,vc2.variant2_RPS v2_normalized_RPS
       ,vc2.variant2_AOV v2_normalized_AOV
       ,vc2.variant2_UPT v2_normalized_UPT
       ,vc2.variant2_AUR v2_normalized_AUR
       ,vc2.variant2_bouncerate	v2_normalized_bouncerate
    from control as c
    left join variant1_calculation as vc1
    on vc1.date = c.date and vc1.bigquery_name = c.bigquery_name and vc1.lookup_platform = c.lookup_platform
    left join variant2_calculation  as vc2
    on vc2.date = c.date and vc2.bigquery_name = c.bigquery_name and vc2.lookup_platform = c.lookup_platform
    left join control_calculation  as cc 
    on cc.date = c.date and cc.bigquery_name = c.bigquery_name and cc.lookup_platform = c.lookup_platform
    order by date


