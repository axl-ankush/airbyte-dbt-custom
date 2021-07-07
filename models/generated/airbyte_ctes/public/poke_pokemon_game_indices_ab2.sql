{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _airbyte_poke_pokemon_hashid,
    cast({{ adapter.quote('version') }} as {{ type_json() }}) as {{ adapter.quote('version') }},
    cast(game_index as {{ dbt_utils.type_bigint() }}) as game_index,
    _airbyte_emitted_at
from {{ ref('poke_pokemon_game_indices_ab1') }}
-- game_indices at poke_pokemon/game_indices

