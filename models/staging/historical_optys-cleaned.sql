SELECT
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

FROM {{ source('dbt', 'historical_optys_raw') }} as historicals 

WHERE team is not null
