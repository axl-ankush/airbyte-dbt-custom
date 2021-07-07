{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _airbyte_held_items_hashid,
    cast(rarity as {{ dbt_utils.type_string() }}) as rarity,
    cast({{ adapter.quote('version') }} as {{ type_json() }}) as {{ adapter.quote('version') }},
    _airbyte_emitted_at
from {{ ref('poke_pokemon_held_items_version_details_ab1') }}
-- version_details at poke_pokemon/held_items/version_details

