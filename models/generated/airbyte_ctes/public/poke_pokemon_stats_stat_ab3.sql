{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to build a hash column based on the values of this record
select
    *,
    {{ dbt_utils.surrogate_key([
        '_airbyte_stats_hashid',
        'url',
        adapter.quote('name'),
    ]) }} as _airbyte_stat_hashid
from {{ ref('poke_pokemon_stats_stat_ab2') }}
-- stat at poke_pokemon/stats/stat

