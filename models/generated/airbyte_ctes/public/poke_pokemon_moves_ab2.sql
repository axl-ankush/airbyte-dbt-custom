{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _airbyte_poke_pokemon_hashid,
    cast({{ adapter.quote('move') }} as {{ type_json() }}) as {{ adapter.quote('move') }},
    version_group_details,
    _airbyte_emitted_at
from {{ ref('poke_pokemon_moves_ab1') }}
-- moves at poke_pokemon/moves

