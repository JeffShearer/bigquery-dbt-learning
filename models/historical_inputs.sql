with wins as (
    select 
        count(id) as count,
        team,
        avg(amount) as asp
    from `lofty-dynamics-283618.dbt.historical_optys_raw` as historicals
where team is not null and stage = 'closed-won'
group by team
),
losses as (
    select 
        count(id) as count,
        team,
        avg(amount) as asp
    from `lofty-dynamics-283618.dbt.historical_optys_raw` as historicals
where team is not null and stage = 'closed-lost'
group by team
),
opens as (
    select 
        count(id) as count,
        team,
        avg(amount) as asp
    from `lofty-dynamics-283618.dbt.historical_optys_raw` as historicals
where team is not null and stage not like 'closed%'
group by team
),

final as (
    select
    case
        when team like 'us%' then 'na'
        else team
    end as team,
    wins.count as won_count,
    losses.count as lost_count,
    opens.count as open_count,

    wins.asp as won_asp,
    losses.asp as lost_asp,
    opens.asp as open_asp,

from wins
join losses using (team)
join opens using (team)
)

select * from final
