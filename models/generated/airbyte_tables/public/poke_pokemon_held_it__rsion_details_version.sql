{{ config(schema="public", tags=["nested"]) }}
-- Final base SQL model
select
    _airbyte_version_details_hashid,
    url,
    {{ adapter.quote('name') }},
    _airbyte_emitted_at,
    _airbyte_version_hashid
from {{ ref('poke_pokemon_held_it__rsion_details_version_ab3') }}
-- version at poke_pokemon/held_items/version_details/version from {{ ref('poke_pokemon_held_items_version_details') }}

