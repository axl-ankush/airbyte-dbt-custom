{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "source1",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('example_table_1_ab3') }}
select
    ct,
    id,
    username,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_example_table_1_hashid
from {{ ref('example_table_1_ab3') }}
-- example_table_1 from {{ source('source1', '_airbyte_raw_example_table_1') }}
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}
{{ config(
    unique_key = 'id',
    post_hook="delete from {{this}} where _airbyte_cdc_deleted_at is not null",
    tags = [ "top-level" ]
) }}

{%- set columns = adapter.get_columns_in_relation(this) -%}

with __dbt__cte_1 as (
  select
  {% for column in columns %}
      {%- if column.name != '_airbyte_emitted_at' %}
          {% if "date" in column.data_type or "time" in column.data_type %}
              CAST(JSON_UNQUOTE(json_extract(_airbyte_data, '$."{{column.name}}"')) as datetime) as {{column.name}}
          {% else %}
              NULLIF(CAST(JSON_UNQUOTE(json_extract(_airbyte_data, '$."{{column.name}}"')) as CHAR),'null') as {{column.name}}
          {% endif %}
      {% else %}
          {{column.name}} as {{column.name}}
      {% endif %}
      {%- if not loop.last %},{% endif %}
  {% endfor %}
  from {{ target.schema }}.{{ var("raw_name") }}{{ model.name }} 
),
__dbt__cte_2 as (
  select
  {% for column in columns %}
      {{column.name}} as {{column.name}},
  {% endfor %}
  ROW_NUMBER() OVER ( PARTITION BY id ORDER BY _airbyte_emitted_at DESC ) AS rn
  from __dbt__cte_1
  where 1=1
  {{ incremental_clause('_airbyte_emitted_at', this) }}
)

-- Handle Deletes
delete from {{this}} where _airbyte_cdc_deleted_at is not null;

-- Handle Updates
update {{this}} dest
set
  {% for column in columns %}
      dest.{{column.name}} = src.{{column.name}},
  {% endfor %}
  dest._airbyte_emitted_at = src._airbyte_emitted_at,
  dest._airbyte_cdc_deleted_at = src._airbyte_cdc_deleted_at
from __dbt__cte_2 src
where dest.id = src.id and src.rn = 1;

-- Handle Inserts
insert into {{this}} (id, username, date)
select
  id as id,
  username as username,
  date as date
from __dbt__cte_2
where 1 = 1 and rn = 1;


