{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _airbyte_abilities_hashid,
    cast(url as {{ dbt_utils.type_string() }}) as url,
    cast({{ adapter.quote('name') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('name') }},
    _airbyte_emitted_at
from {{ ref('poke_pokemon_abilities_ability_ab1') }}
-- ability at poke_pokemon/abilities/ability

