
with 
    -- join monthly revenue with historical inputs
    inputs as (
        select
            team,
            historical_inputs.region,
            date(date) as month,
            closed_won_asp,
            win_rate,
            -- split na target by 50% to account for us east/west split
            case
                when historical_inputs.region = 'na' then revenue_target/2
                else revenue_target
            end as revenue_target

        from {{ ref('stg_historical_inputs') }} as historical_inputs

        inner join {{ ref('stg_rev_seasonality_targets') }} as monthly_revenue using(region)
    ),
    --calculate won deals & the total opp creation needed for a given month
    calculations as (
        select
            team,
            region,
            month,
            revenue_target,
            revenue_target / closed_won_asp as won_deals_target,
            (revenue_target / closed_won_asp)/win_rate as opps_needed,

        from inputs
    ),
    -- calculates opps needed based on percentage of conversions in n month from historical inputs
    month0 as (
        select
            team,
            region,
            month,
            opps_needed*pct_conversions_num_month_0 as opps_needed,
        from calculations c
        inner join {{ ref('stg_historical_inputs') }} using (team,region)
    ),
    -- for month 1-4, decrements close month and uses relevant percentage from inputs table
    month1 as (
        select
        team,
        region,
        date({{dbt_utils.dateadd(datepart='month', interval='-1', from_date_or_timestamp='month') }}) as month,
        opps_needed*pct_conversions_num_month_1 as opps_needed
        from calculations
        inner join {{ ref('stg_historical_inputs') }} using (team,region)

    ),
    month2 as (
        select
        team,
        region,
        date({{dbt_utils.dateadd(datepart='month', interval='-2', from_date_or_timestamp='month') }}) as month,
        opps_needed*pct_conversions_num_month_2 as opps_needed
        from calculations
        inner join {{ ref('stg_historical_inputs') }} using (team,region)

    ),
    month3 as (
        select
        team,
        region,
        date({{dbt_utils.dateadd(datepart='month', interval='-3', from_date_or_timestamp='month') }}) as month,
        opps_needed*pct_conversions_num_month_3 as opps_needed
        from calculations
        inner join {{ ref('stg_historical_inputs') }} using (team,region)

    ),
    month4 as (
        select
        team,
        region,
        date({{dbt_utils.dateadd(datepart='month', interval='-4', from_date_or_timestamp='month') }}) as month,
        opps_needed*pct_conversions_num_month_4 as opps_needed
        from calculations
        inner join {{ ref('stg_historical_inputs') }} using (team,region)

    ),
    -- union all monthly tables into one
    combined as (
        select * from month0
        union distinct
        select * from month1
        union distinct
        select * from month2
        union distinct
        select * from month3
        union distinct
        select * from month1
    ),
    -- then flatten to get a new monthly total opps needed, which is a factor of the revenue needed for that month and for future months
    flattened as (
        select 
        c.team,
        c.region,
        c.month,
        sum(c.opps_needed) as opps_needed
        from combined c
        left join calculations using(team,month)
        group by c.region,c.team,c.month
    ),
    -- bring creation targets in with revenue targets for a final goals
    final as (
        select
            f.team,
            f.region,
            f.month,
            revenue_target,
            won_deals_target,
            f.opps_needed
        from flattened f
        left join calculations using(team,month)
        
    )

select * from final
order by team, month

