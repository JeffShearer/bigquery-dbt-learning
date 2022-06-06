
-- join monthly revenue with historical inputs
with 
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

        inner join {{ ref('rev_seasonality_targets') }} as monthly_revenue on historical_inputs.region = monthly_revenue.region
    ),
    --calculate upfunnel goals using inputs & targets
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
            union distinct
            select team,region,create_month as date from calculations
        ),
        -- select just the revenue-stage columns
        revenue as (
            select 
                team,
                region,
                revenue_date,
                revenue_target,
                won_deals_target
            from calculations
        ),
        -- select just the opp creation-stage columns
        created as (
            select 
                team,
                region,
                create_month,
                created_opps_target
            from calculations
        ),
-- combine dates table with revenue & created columns to get final output
        final as (
        select
            dates.team,
            dates.region,
            date,
            created_opps_target,
            revenue_target,
            won_deals_target

        from dates
        left join created on date = create_month and dates.team = created.team
        left join revenue on date = revenue_date and dates.team = revenue.team
        )

select * from final order by date


