with wins as (
    select 
        id,
        team,
        stage,
        amount
    from `lofty-dynamics-283618.dbt.historical_optys_raw` as historicals
where team is not null and stage = 'closed-won'
),
losses as (
    select 
        id,
        team,
        stage,
        amount
    from `lofty-dynamics-283618.dbt.historical_optys_raw` as historicals
where team is not null and stage = 'closed-lost'
),
opens as (
    select 
        id,
        team,
        stage,
        amount
    from `lofty-dynamics-283618.dbt.historical_optys_raw` as historicals
where team is not null and stage not like 'closed%'
),

final as (
    select
    team,
    avg(wins.amount) as won_asp,
    avg(losses.amount) as lost_asp,
    avg(opens.amount) as open_asp,

from wins
join losses using (team)
join opens using (team)

group by team
)

select * from final
