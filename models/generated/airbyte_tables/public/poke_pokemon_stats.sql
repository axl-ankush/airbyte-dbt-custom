{{ config(schema="public", tags=["nested"]) }}
-- Final base SQL model
select
    _airbyte_poke_pokemon_hashid,
    stat,
    effort,
    base_stat,
    _airbyte_emitted_at,
    _airbyte_stats_hashid
from {{ ref('poke_pokemon_stats_ab3') }}
-- stats at poke_pokemon/stats from {{ ref('poke_pokemon') }}

