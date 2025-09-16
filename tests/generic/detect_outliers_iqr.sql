-- tests/generic/test_outliers_iqr.sql
{% test outliers_iqr(model, column_name, multiplier=1.5) %}

with quartiles as (
  select
    percentile_cont(0.25) within group (order by {{ column_name }}) as q1,
    percentile_cont(0.75) within group (order by {{ column_name }}) as q3
  from {{ model }}
  where {{ column_name }} is not null
),

bounds as (
  select
    q1 - ({{ multiplier }} * (q3 - q1)) as lower_bound,
    q3 + ({{ multiplier }} * (q3 - q1)) as upper_bound
  from quartiles
)

select *
from {{ model }}
cross join bounds
where {{ column_name }} < lower_bound 
   or {{ column_name }} > upper_bound

{% endtest %}