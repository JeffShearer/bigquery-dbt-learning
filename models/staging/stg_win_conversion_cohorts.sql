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
        count(1) as num_wins
    from cohort_items
    group by 1
    order by 1
),

--each month for each cohort and their related num_wins (numerator for final)
wins_by_month as (
    select 
        team,
        month_num,
        count(1) as num_wins
    from cohort_items
    group by 1,2
    order by 1,2
),

-- cohorts with the percentage of wins by future month_num
final as (
    select
        b.team,
        s.num_wins as total_wins,
        b.num_wins as month_num_wins,
        b.month_num,
        b.num_wins / s.num_wins as percentage_of_wins
    from wins_by_month b
    left join cohort_size s using(team)
    order by 1,4
)

select * from final