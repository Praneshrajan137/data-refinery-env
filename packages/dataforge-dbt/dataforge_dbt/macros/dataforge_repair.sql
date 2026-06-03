{% macro dataforge_repair(column_name, mode='dry_run') %}
  {% set valid_modes = ['dry_run', 'apply', 'refuse'] %}
  {% if mode not in valid_modes %}
    {{ exceptions.raise_compiler_error("dataforge_repair mode must be one of: dry_run, apply, refuse") }}
  {% endif %}
  {% set dispatch_command = "dataforge-dbt --relation " ~ this ~ " --column " ~ column_name ~ " --mode " ~ mode ~ " --target-path target --project-dir ." %}
  {% do log("DATAFORGE_DBT dispatch_configured relation=" ~ this ~ " column=" ~ column_name ~ " mode=" ~ mode ~ " command=" ~ dispatch_command, info=True) %}
  select 1 as dataforge_repair_hook
{% endmacro %}
