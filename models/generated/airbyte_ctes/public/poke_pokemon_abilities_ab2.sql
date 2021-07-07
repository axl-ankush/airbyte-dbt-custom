{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _airbyte_poke_pokemon_hashid,
    cast(slot as {{ dbt_utils.type_bigint() }}) as slot,
    cast(ability as {{ type_json() }}) as ability,
    {{ cast_to_boolean('is_hidden') }} as is_hidden,
    _airbyte_emitted_at
from {{ ref('poke_pokemon_abilities_ab1') }}
-- abilities at poke_pokemon/abilities

