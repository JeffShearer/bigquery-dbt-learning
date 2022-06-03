SELECT *,
{{dbt_utils.dateadd(datepart='day', interval='age', from_date_or_timestamp='created') }} as closed

FROM `lofty-dynamics-283618.dbt.historical_optys_raw` as historicals 

WHERE team is not null
