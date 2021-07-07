{{ config(schema="public", tags=["nested"]) }}
-- Final base SQL model
select
    _airbyte_game_indices_hashid,
    url,
    {{ adapter.quote('name') }},
    _airbyte_emitted_at,
    _airbyte_version_hashid
from {{ ref('poke_pokemon_game_indices_version_ab3') }}
-- version at poke_pokemon/game_indices/version from {{ ref('poke_pokemon_game_indices') }}

