{{ config(schema="_airbyte_public", tags=["top-level-intermediate"]) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    cast({{ adapter.quote('id') }} as {{ dbt_utils.type_bigint() }}) as {{ adapter.quote('id') }},
    cast({{ adapter.quote('name') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('name') }},
    forms,
    moves,
    cast({{ adapter.quote('order') }} as {{ dbt_utils.type_bigint() }}) as {{ adapter.quote('order') }},
    stats,
    {{ adapter.quote('types') }},
    cast(height as {{ dbt_utils.type_bigint() }}) as height,
    cast(weight as {{ dbt_utils.type_bigint() }}) as weight,
    cast(species as {{ type_json() }}) as species,
    cast(sprites as {{ type_json() }}) as sprites,
    abilities,
    held_items,
    {{ cast_to_boolean(adapter.quote('is_default ')) }} as {{ adapter.quote('is_default ') }},
    game_indices,
    cast(base_experience as {{ dbt_utils.type_bigint() }}) as base_experience,
    cast(location_area_encounters as {{ dbt_utils.type_string() }}) as location_area_encounters,
    _airbyte_emitted_at
from {{ ref('poke_pokemon_ab1') }}
-- poke_pokemon

