{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _airbyte_poke_pokemon_hashid,
    cast(stat as {{ type_json() }}) as stat,
    cast(effort as {{ dbt_utils.type_bigint() }}) as effort,
    cast(base_stat as {{ dbt_utils.type_bigint() }}) as base_stat,
    _airbyte_emitted_at
from {{ ref('poke_pokemon_stats_ab1') }}
-- stats at poke_pokemon/stats

