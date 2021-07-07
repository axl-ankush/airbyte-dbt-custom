{{ config(schema="public", tags=["nested"]) }}
-- Final base SQL model
select
    _airbyte_poke_pokemon_hashid,
    {{ adapter.quote('version') }},
    game_index,
    _airbyte_emitted_at,
    _airbyte_game_indices_hashid
from {{ ref('poke_pokemon_game_indices_ab3') }}
-- game_indices at poke_pokemon/game_indices from {{ ref('poke_pokemon') }}

