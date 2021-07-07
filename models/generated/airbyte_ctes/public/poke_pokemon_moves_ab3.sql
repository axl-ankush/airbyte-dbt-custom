{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to build a hash column based on the values of this record
select
    *,
    {{ dbt_utils.surrogate_key([
        '_airbyte_poke_pokemon_hashid',
        adapter.quote('move'),
        array_to_string('version_group_details'),
    ]) }} as _airbyte_moves_hashid
from {{ ref('poke_pokemon_moves_ab2') }}
-- moves at poke_pokemon/moves

