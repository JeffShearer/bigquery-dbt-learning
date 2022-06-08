{% set stages = ["closed_won", "closed_lost", "open","discovery","qualified"] %}
-- sets team & regional inputs for asp & win rates
with calculations as (
        select
            {% for value in stages %}
            avg(case when stage = '{{value}}' then amount end) as {{value}}_asp,
            count(case when stage = '{{value}}' then id end) as {{value}}_count,
            {% endfor %}
            team,
            case
                when team like 'us-%' then 'na'
                else team
                end as region,
        from {{ ref('stg_historical_optys_cleaned') }}
        where team is not null

        group by team),

        -- joins cohort conversion data by month to replace old cycle time metrics
    cohorts as (
        select
            i.team,
            i.region,
            cast(closed_won_asp as numeric) as closed_won_asp,
            cast(closed_lost_asp as numeric) as closed_lost_asp,
            cast(closed_won_count/(closed_won_count + closed_lost_count) as numeric) as win_rate,
            pct_conversions_num_month_0,
            pct_conversions_num_month_1,
            pct_conversions_num_month_2,
            pct_conversions_num_month_3,
            pct_conversions_num_month_4
        from calculations as i
        inner join {{ref('stg_win_conversion_cohorts')}} using(team)
    )

select * from cohorts