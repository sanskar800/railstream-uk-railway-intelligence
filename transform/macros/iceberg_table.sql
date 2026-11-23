{#
  Materialize a model as an Iceberg table through DuckDB (ADR-017).

  dbt's standard `table` builds `<name>__dbt_tmp` and renames it into place; DuckDB-Iceberg
  refuses that rename, and it has no CREATE OR REPLACE. Instead:
  - first build (or --full-refresh): CREATE TABLE ... AS
  - every later build: DELETE all rows and INSERT the new result in ONE transaction, which
    Iceberg commits as a single snapshot. Readers see the old or the new table, never an empty
    or half-built one. A changed column list needs --full-refresh (INSERT ... BY NAME fails).
  The statements run inside dbt's transaction and the materialization must commit it, as dbt's
  own materializations do: without `adapter.commit()` DuckDB rolls the work back at close.
#}
{% materialization iceberg_table, adapter='duckdb' %}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set existing = adapter.get_relation(database=this.database, schema=this.schema,
                                          identifier=this.identifier) -%}
  {{ run_hooks(pre_hooks) }}
  {% if existing is not none and should_full_refresh() %}
    {% call statement('drop_for_full_refresh') %}drop table {{ target_relation }}{% endcall %}
    {% set existing = none %}
  {% endif %}
  {% if existing is none %}
    {% call statement('create_schema') %}
      create schema if not exists {{ target_relation.database }}.{{ target_relation.schema }}
    {% endcall %}
    {% call statement('main') %}create table {{ target_relation }} as {{ sql }}{% endcall %}
  {% else %}
    {% call statement('main') %}
      delete from {{ target_relation }};
      insert into {{ target_relation }} by name {{ sql }}
    {% endcall %}
  {% endif %}
  {% do adapter.commit() %}
  {{ run_hooks(post_hooks) }}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
