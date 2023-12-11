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

