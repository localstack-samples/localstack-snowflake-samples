{#
  dbt run-operation drop_pr_schema --target pr
  Drops this pull request's schema. CI runs it before the PR build (fresh
  state on every push) and, on a real account, when the PR closes.
#}
{% macro drop_pr_schema() %}
    {%- set relation = api.Relation.create(database=target.database, schema=pr_schema_name()) -%}
    {% do log("Dropping schema " ~ relation, info=true) %}
    {% do adapter.drop_schema(relation) %}
{% endmacro %}
