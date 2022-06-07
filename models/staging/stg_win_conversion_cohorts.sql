-- based on https://www.holistics.io/blog/calculate-cohort-retention-analysis-with-sql/
-- grab all the wins with the month they were created (their cohort) and difference between creation and close in months (0 = same month as creation)
with cohort_items as (
    select 
        id,
        team,
        closed,
        ({{ dbt_date.date_part(datepart='month', date='created') }}) as cohort_month,
        ({{ dbt_date.date_part(datepart='year', date='created') }}) as cohort_year,
        {{ dbt_utils.datediff("created", "closed", 'month') }} as month_num,

    from {{ref('stg_historical_optys_cleaned')}}

    where stage = 'closed_won'  order by cohort_month
    ), 

-- Count the total size of each cohort for use in the denominator of final
cohort_size as (
    select 
        team,
        cohort_year,
        cohort_month,
        count(1) as num_wins
    from cohort_items
    group by 1,2,3
    order by 1,2,3
),

--each month for each cohort and their related num_wins (numerator for final)
wins_by_month as (
    select 
        team,
        cohort_year,
        cohort_month,
        month_num,
        count(1) as num_wins
    from cohort_items
    group by 1,2,3,4
    order by 1,2,3,4
),

-- cohorts with the percentage of wins by future month_num
final as (
    select
        b.team,
        b.cohort_year,
        b.cohort_month,
        s.num_wins as total_wins,
        b.num_wins as month_wins,
        b.month_num,
        b.num_wins / s.num_wins as percentage
    from wins_by_month b
    left join cohort_size s using(cohort_month,cohort_year, team)
    order by 1,2,3,6
),
-- this doesn't really work yet.
summary as (
    select 
        team,
        month_num,
        sum(month_wins) as month_wins,
        sum(total_wins) as total_wins,
    from final
    group by 1,2
    order by 1,2
)

select * from final