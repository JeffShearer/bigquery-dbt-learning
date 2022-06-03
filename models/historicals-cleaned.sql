SELECT *,
{{dbt_utils.dateadd(datepart='day', interval='age', from_date_or_timestamp='created') }} as closed
FROM `lofty-dynamics-283618.demand_model.historical_optys` as historicals 

WHERE team is not null

