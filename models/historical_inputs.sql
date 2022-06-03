{% set stages = ["closed_won", "closed_lost", "open","discovery","qualified"] %}

with 
    summary1 as (
        select
            {% for value in stages %}
            avg(case when stage = '{{value}}' then amount end) as {{value}}_asp,
            count(case when stage = '{{value}}' then id end) as {{value}}_count,
            {% endfor %}
            team
        from {{ ref('historical_optys-cleaned') }}
        where team is not null

        group by team),

    final as (
        select
        team,
        closed_won_asp,
        closed_lost_asp,
        closed_won_count/(closed_won_count + closed_lost_count) as win_rate
        from summary1
    )

select * from final