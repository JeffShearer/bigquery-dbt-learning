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
        from {{ ref('historical_optys-cleaned') }}
        where team is not null

        group by team),

    final as (
        select
        team,
        region,
        round(closed_won_asp,2) as closed_won_asp,
        round(closed_lost_asp,2) as closed_lost_asp,
        round(closed_won_count/(closed_won_count + closed_lost_count),2) as win_rate,
        --cycle time needs to be an int for dateadd function to work in goals model
        cast(created_closed_cycle_time as INT64) as created_closed_cycle_time
        from calculations
    )

select * from final