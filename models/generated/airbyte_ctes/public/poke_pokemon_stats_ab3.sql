{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to build a hash column based on the values of this record
select
    *,
    {{ dbt_utils.surrogate_key([
        '_airbyte_poke_pokemon_hashid',
        'stat',
        'effort',
        'base_stat',
    ]) }} as _airbyte_stats_hashid
from {{ ref('poke_pokemon_stats_ab2') }}
-- stats at poke_pokemon/stats

