{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _airbyte_poke_pokemon_hashid,
    cast(item as {{ type_json() }}) as item,
    version_details,
    _airbyte_emitted_at
from {{ ref('poke_pokemon_held_items_ab1') }}
-- held_items at poke_pokemon/held_items

