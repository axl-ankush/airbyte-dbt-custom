{{ config(schema="public", tags=["nested"]) }}
-- Final base SQL model
select
    _airbyte_poke_pokemon_hashid,
    item,
    version_details,
    _airbyte_emitted_at,
    _airbyte_held_items_hashid
from {{ ref('poke_pokemon_held_items_ab3') }}
-- held_items at poke_pokemon/held_items from {{ ref('poke_pokemon') }}

