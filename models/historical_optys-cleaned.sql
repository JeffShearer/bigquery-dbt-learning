SELECT
id,
company,
{{dbt_utils.dateadd(datepart='day', interval='age', from_date_or_timestamp='created') }} as closed,
team,
case when amount is null then 0 else amount end as amount,
replace(stage,'-','_') as stage,
age

FROM `lofty-dynamics-283618.dbt.historical_optys_raw` as historicals 

WHERE team is not null
