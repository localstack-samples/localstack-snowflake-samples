{#
  Where each node is built:

  * pr (and cloud with DBT_PR_NUMBER set): everything goes into one schema,
    pr_<number>. A pull request owns exactly one schema, so two open PRs never
    collide and cleanup is a single DROP SCHEMA.
  * prod (and cloud without a PR number): the layer's custom schema is used
    as is: staging, intermediate, marts, snapshots, reference.
  * dev and anything else: <target.schema>_<custom schema>, a personal sandbox.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set pr_number = env_var('DBT_PR_NUMBER', '') | trim -%}
    {%- if target.name == 'pr' or (target.name == 'cloud' and pr_number) -%}
        {{ pr_schema_name() }}
    {%- elif target.name in ['prod', 'cloud'] and custom_schema_name -%}
        {{ custom_schema_name | trim }}
    {%- elif custom_schema_name -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}

{% macro pr_schema_name() -%}
    {%- set pr_number = env_var('DBT_PR_NUMBER', '') | trim -%}
    {%- if not pr_number -%}
        {{ exceptions.raise_compiler_error("DBT_PR_NUMBER must be set to build a pull request schema") }}
    {%- endif -%}
    pr_{{ pr_number | lower | replace('-', '_') }}
{%- endmacro %}
