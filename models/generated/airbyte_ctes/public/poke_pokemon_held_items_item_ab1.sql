{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    _airbyte_held_items_hashid,
    {{ json_extract_scalar('item', ['url']) }} as url,
    {{ json_extract_scalar('item', ['name']) }} as {{ adapter.quote('name') }},
    _airbyte_emitted_at
from {{ ref('poke_pokemon_held_items') }}
where item is not null
-- item at poke_pokemon/held_items/item

