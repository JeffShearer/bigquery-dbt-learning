-- combines seasonality with targets to create revenue targets by team and by month
select 
    seasonality.year,
    case
        when month = 'January' then concat(seasonality.year,"-","01","-","01")
        when month = 'February' then concat(seasonality.year,"-","02","-","01")
        when month = 'March' then concat(seasonality.year,"-","03","-","01")
        when month = 'April' then concat(seasonality.year,"-","04","-","01")
        when month = 'May' then concat(seasonality.year,"-","05","-","01")
        when month = 'June' then concat(seasonality.year,"-","06","-","01")
        when month = 'July' then concat(seasonality.year,"-","07","-","01")
        when month = 'August' then concat(seasonality.year,"-","08","-","01")
        when month = 'September' then concat(seasonality.year,"-","09","-","01")
        when month = 'October' then concat(seasonality.year,"-","10","-","01")
        when month = 'November' then concat(seasonality.year,"-","11","-","01")
        when month = 'December' then concat(seasonality.year,"-","12","-","01")
    end as date,
    region,
    -- need to increase targets to make upfunnel goals make sense
    cast(rev_percentage as numeric) * (rev_target*300) as revenue_target

from {{ source('dbt', 'revenue_seasonality_raw') }} as seasonality

left join {{ source('dbt', 'revenue_targets_raw') }} as targets on seasonality.year = targets.year