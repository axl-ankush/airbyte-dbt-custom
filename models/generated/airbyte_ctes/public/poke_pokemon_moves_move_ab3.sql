{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to build a hash column based on the values of this record
select
    *,
    {{ dbt_utils.surrogate_key([
        '_airbyte_moves_hashid',
        'url',
        adapter.quote('name'),
    ]) }} as _airbyte_move_hashid
from {{ ref('poke_pokemon_moves_move_ab2') }}
-- move at poke_pokemon/moves/move

