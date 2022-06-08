with basic as (
    select
        id,
        company,
        --generate close date based on created date + age
        {{dbt_utils.dateadd(datepart='day', interval='age', from_date_or_timestamp='created') }} as closed,
        team,
        --populate null amounts with 0
        case when amount is null then 0 else amount end as amount,
        --normalize stage names to prevent errors with jinja
        replace(stage,'-','_') as stage,
        created,
        age

    from {{ source('dbt', 'historical_optys_raw') }} as historicals 

    where team is not null
),

date_clean as (
    select
        id,
        company,
        cast (closed as date) as closed,
        team,
        amount,
        stage,
        created,
        age

    from basic
)
select * from date_clean