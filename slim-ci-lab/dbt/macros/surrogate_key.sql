{#
  Deterministic surrogate key: MD5 over the columns' text, with NULLs made
  explicit so (NULL, 'a') and ('', 'a') hash differently. Same idea as
  dbt_utils.generate_surrogate_key, without the package dependency.
#}
{% macro surrogate_key(columns) -%}
    md5(
        {%- for column in columns %}
        coalesce(cast({{ column }} as varchar), '_null_'){% if not loop.last %} || '|' ||{% endif %}
        {%- endfor %}
    )
{%- endmacro %}
