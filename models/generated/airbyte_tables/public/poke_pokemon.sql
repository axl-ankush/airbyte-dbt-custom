{{ config(schema="public", tags=["top-level"]) }}
-- Final base SQL model
select
    {{ adapter.quote('id') }},
    {{ adapter.quote('name') }},
    forms,
    moves,
    {{ adapter.quote('order') }},
    stats,
    {{ adapter.quote('types') }},
    height,
    weight,
    species,
    sprites,
    abilities,
    held_items,
    {{ adapter.quote('is_default ') }},
    game_indices,
    base_experience,
    location_area_encounters,
    _airbyte_emitted_at,
    _airbyte_poke_pokemon_hashid
from {{ ref('poke_pokemon_ab3') }}
-- poke_pokemon from {{ source('public', '_airbyte_raw_poke_pokemon') }}

