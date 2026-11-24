{#
  Rolling rebuild window for gold (ADR-018). Every build recomputes only recent UK local dates
  and replaces exactly those rows; older dates are final. This keeps a build's memory and time
  flat however long silver grows: recomputing all history every 15 minutes outgrew the 3 GB
  Airflow container after 19 days (findings F32).

  - Default: the last `rebuild_days` (3) local dates up to now, enough for late corrections.
  - Backfill: --vars {"rebuild_from":"2026-11-05","rebuild_to":"2026-11-12"} rebuilds
    [from, to); `make gold-backfill FROM=...` walks history in 7-day chunks.
#}
{% macro rebuild_from() -%}
  {%- set explicit = var('rebuild_from', none) -%}
  {%- if explicit -%}
    DATE '{{ explicit }}'
  {%- else -%}
    {%- set today = modules.datetime.datetime.now(modules.pytz.timezone('Europe/London')).date() -%}
    DATE '{{ today - modules.datetime.timedelta(days=var('rebuild_days', 3) - 1) }}'
  {%- endif -%}
{%- endmacro %}

{% macro rebuild_to() -%}
  {%- set explicit = var('rebuild_to', none) -%}
  {%- if explicit -%}DATE '{{ explicit }}'{%- endif -%}
{%- endmacro %}

{# A predicate on a UK local date column: true inside the window being rebuilt. #}
{% macro in_rebuild_window(date_expr) -%}
  {{ date_expr }} >= {{ rebuild_from() }}
  {%- if rebuild_to() %} and {{ date_expr }} < {{ rebuild_to() }}{% endif -%}
{%- endmacro %}

{#
  A predicate on silver's UTC actual_ts covering the window plus `before_days` earlier (one
  extra day absorbs the UK/UTC offset), and one day after an explicit end. Filtering actual_ts
  lets Iceberg skip whole day partitions of silver.
#}
{% macro silver_window(ts_expr, before_days=0) -%}
  {{ ts_expr }} >= cast({{ rebuild_from() }} as timestamp) - interval {{ before_days + 1 }} day
  {%- if rebuild_to() %} and {{ ts_expr }} < cast({{ rebuild_to() }} as timestamp) + interval 1 day{% endif -%}
{%- endmacro %}
