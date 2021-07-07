{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _airbyte_poke_pokemon_hashid,
    cast(slot as {{ dbt_utils.type_bigint() }}) as slot,
    cast({{ adapter.quote('type') }} as {{ type_json() }}) as {{ adapter.quote('type') }},
    _airbyte_emitted_at
from {{ ref('poke_pokemon_types_ab1') }}
-- types at poke_pokemon/types

