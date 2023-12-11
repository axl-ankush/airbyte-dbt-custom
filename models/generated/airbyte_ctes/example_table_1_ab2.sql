{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_source1",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('example_table_1_ab1') }}
select
        case when ct regexp '\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.*' THEN STR_TO_DATE(SUBSTR(ct, 1, 19), '%Y-%m-%dT%H:%i:%S')
        else cast(if(ct = '', NULL, ct) as datetime)
        end as ct
        ,
    cast(id as {{ dbt_utils.type_bigint() }}) as id,
    cast(username as {{ dbt_utils.type_string() }}(1024)) as username,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('example_table_1_ab1') }}
-- example_table_1
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

