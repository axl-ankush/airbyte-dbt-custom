{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _airbyte_version_details_hashid,
    cast(url as {{ dbt_utils.type_string() }}) as url,
    cast({{ adapter.quote('name') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('name') }},
    _airbyte_emitted_at
from {{ ref('poke_pokemon_held_it__rsion_details_version_ab1') }}
-- version at poke_pokemon/held_items/version_details/version

