-- build monthly revenue by region by joining seasonality & revenue targets tables
with monthly_revenue as (
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
        rev_percentage * (rev_target*300) as revenue_target

    from `lofty-dynamics-283618.dbt.revenue_seasonality_raw` as seasonality

    left join `lofty-dynamics-283618.dbt.revenue_targets_raw` as targets on seasonality.year = targets.year
    ),

-- join monthly revenue with historical inputs
inputs as (
    select
        team,
        historical_inputs.region,
        date(date) as revenue_date,
        created_closed_cycle_time,
        closed_won_asp,
        win_rate,
        --determine opp create by subtracting cycle times
        {{dbt_utils.dateadd(datepart='day', interval='-created_closed_cycle_time', from_date_or_timestamp='date') }} as create_date,
        -- split na target by 50% to account for us east/west split
        case
            when historical_inputs.region = 'na' then revenue_target/2
            else revenue_target
        end as revenue_target

    from {{ ref('historical_inputs') }}

    inner join monthly_revenue on historical_inputs.region = monthly_revenue.region
),
    --calculate upfunnel dates & targets
    calculations as (
        select
            team,
            region,
            revenue_date,
            revenue_target,
            revenue_target / closed_won_asp as won_deals_target,
            (revenue_target / closed_won_asp)/win_rate as created_opps_target,
            --truncates create_date to create month, then converts to date format (vs datetime)
            date({{ dbt_utils.date_trunc(datepart='month', date='create_date') }}) as create_month

        from inputs
    ),
        -- select all possible dates from revenue & created date columns
        dates as (
            select team, region, revenue_date as date from calculations
        union all
            select team,region,create_month as date from calculations
        ),
        -- select just the revenue-related columns
        revenue as (
            select 
                team,
                region,
                revenue_date,
                revenue_target,
                won_deals_target
            from calculations
        ),
        -- select just the opp creation-related columns
        created as (
            select 
                team,
                region,
                create_month,
                created_opps_target
            from calculations
        )
-- combine dates table with revenue & created columns to get final output
select
    dates.team,
    dates.region,
    date,
    created_opps_target,
    revenue_target,
    won_deals_target

from dates
left join revenue on date = revenue_date
left join created on date = create_month


