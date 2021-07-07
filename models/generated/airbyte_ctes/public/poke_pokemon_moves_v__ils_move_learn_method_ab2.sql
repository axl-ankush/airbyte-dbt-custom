{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _airbyte_version_group_details_hashid,
    cast(url as {{ dbt_utils.type_string() }}) as url,
    cast({{ adapter.quote('name') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('name') }},
    _airbyte_emitted_at
from {{ ref('poke_pokemon_moves_v__ils_move_learn_method_ab1') }}
-- move_learn_method at poke_pokemon/moves/version_group_details/move_learn_method

