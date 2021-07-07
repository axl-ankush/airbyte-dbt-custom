{{ config(schema="public", tags=["nested"]) }}
-- Final base SQL model
select
    _airbyte_held_items_hashid,
    rarity,
    {{ adapter.quote('version') }},
    _airbyte_emitted_at,
    _airbyte_version_details_hashid
from {{ ref('poke_pokemon_held_items_version_details_ab3') }}
-- version_details at poke_pokemon/held_items/version_details from {{ ref('poke_pokemon_held_items') }}

