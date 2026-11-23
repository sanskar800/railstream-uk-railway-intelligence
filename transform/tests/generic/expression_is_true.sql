{# Fails on every row where the expression is false (or null, which also means a broken rule). #}
{% test expression_is_true(model, expression) %}
select *
from {{ model }}
where not coalesce(({{ expression }}), false)
{% endtest %}
