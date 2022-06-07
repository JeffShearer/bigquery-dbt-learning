{% set stages = ["closed_won", "closed_lost", "open","discovery","qualified"] %}

with 
    calculations as (
        select
            {% for value in stages %}
            avg(case when stage = '{{value}}' then amount end) as {{value}}_asp,
            count(case when stage = '{{value}}' then id end) as {{value}}_count,
            {% endfor %}
            avg(case 
                when stage = 'closed_won' then age
            end) as created_closed_cycle_time,
            team,
            case
                when team like 'us-%' then 'na'
                else team
                end as region,
        from {{ ref('stg_historical_optys_cleaned') }}
        where team is not null

        group by team),
        -- not working just yet
        cohorts as (
            select
                i.team,
                i.region,
                i.closed_won_asp,
                i.closed_lost_asp,
                case when month_num = 0 then percentage_of_wins end as wins_month_0,
                case when month_num = 1 then percentage_of_wins end as wins_month_1,
                case when month_num = 2 then percentage_of_wins end as wins_month_2,
                case when month_num = 3 then percentage_of_wins end as wins_month_3,
                case when month_num = 4 then percentage_of_wins end as wins_month_4,
            from calculations as i
            inner join {{ref('stg_win_conversion_cohorts')}} using(team)
        ),

    final as (
        select
        team,
        region,
        cast(closed_won_asp as numeric) as closed_won_asp,
        cast(closed_lost_asp as numeric) as closed_lost_asp,
        cast(closed_won_count/(closed_won_count + closed_lost_count) as numeric) as win_rate,
        --cycle time needs to be an int for dateadd function to work in goals model
        cast(created_closed_cycle_time as INT64) as created_closed_cycle_time
        from calculations
    )

select * from final