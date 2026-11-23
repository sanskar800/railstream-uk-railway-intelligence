{# Fails if two rows share the given columns: the model's grain is not what it claims. #}
{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as rows_with_this_key
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
