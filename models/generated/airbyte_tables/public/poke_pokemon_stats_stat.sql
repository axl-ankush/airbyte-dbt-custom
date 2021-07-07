{{ config(schema="public", tags=["nested"]) }}
-- Final base SQL model
select
    _airbyte_stats_hashid,
    url,
    {{ adapter.quote('name') }},
    _airbyte_emitted_at,
    _airbyte_stat_hashid
from {{ ref('poke_pokemon_stats_stat_ab3') }}
-- stat at poke_pokemon/stats/stat from {{ ref('poke_pokemon_stats') }}

