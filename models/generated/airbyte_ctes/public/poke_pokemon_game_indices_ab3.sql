{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to build a hash column based on the values of this record
select
    *,
    {{ dbt_utils.surrogate_key([
        '_airbyte_poke_pokemon_hashid',
        adapter.quote('version'),
        'game_index',
    ]) }} as _airbyte_game_indices_hashid
from {{ ref('poke_pokemon_game_indices_ab2') }}
-- game_indices at poke_pokemon/game_indices

