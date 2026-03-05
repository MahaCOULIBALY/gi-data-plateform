-- macros/generate_schema_name.sql
-- Override dbt default : utilise le schema configuré tel quel (gld_commercial, gld_performance, etc.)
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
