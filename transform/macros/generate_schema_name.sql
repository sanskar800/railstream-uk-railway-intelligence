{# Use the configured schema as is (`gold`), not dbt's default `<target>_<schema>`. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ (custom_schema_name or target.schema) | trim }}
{%- endmacro %}
