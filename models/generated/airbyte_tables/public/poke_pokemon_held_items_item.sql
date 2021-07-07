{{ config(schema="public", tags=["nested"]) }}
-- Final base SQL model
select
    _airbyte_held_items_hashid,
    url,
    {{ adapter.quote('name') }},
    _airbyte_emitted_at,
    _airbyte_item_hashid
from {{ ref('poke_pokemon_held_items_item_ab3') }}
-- item at poke_pokemon/held_items/item from {{ ref('poke_pokemon_held_items') }}

