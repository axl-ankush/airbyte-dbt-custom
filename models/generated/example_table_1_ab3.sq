{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_source1",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('example_table_1_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'ct',
        'id',
        'username',
    ]) }} as _airbyte_example_table_1_hashid,
    tmp.*
from {{ ref('example_table_1_ab2') }} tmp
-- example_table_1
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

