{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to build a hash column based on the values of this record
select
    *,
    {{ dbt_utils.surrogate_key([
        '_airbyte_poke_pokemon_hashid',
        'slot',
        adapter.quote('type'),
    ]) }} as _airbyte_types_hashid
from {{ ref('poke_pokemon_types_ab2') }}
-- types at poke_pokemon/types

