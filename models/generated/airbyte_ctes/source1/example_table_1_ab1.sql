{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_source1",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('source1', '_airbyte_raw_example_table_1') }}
select
    {{ json_extract_scalar('_airbyte_data', ['ct'], ['ct']) }} as ct,
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as id,
    {{ json_extract_scalar('_airbyte_data', ['username'], ['username']) }} as username,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('source1', '_airbyte_raw_example_table_1') }} as table_alias
-- example_table_1
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

