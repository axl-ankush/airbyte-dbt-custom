{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
{{ unnest_cte('poke_pokemon', 'poke_pokemon', 'stats') }}
select
    _airbyte_poke_pokemon_hashid,
    {{ json_extract(unnested_column_value('stats'), ['stat']) }} as stat,
    {{ json_extract_scalar(unnested_column_value('stats'), ['effort']) }} as effort,
    {{ json_extract_scalar(unnested_column_value('stats'), ['base_stat']) }} as base_stat,
    _airbyte_emitted_at
from {{ ref('poke_pokemon') }}
{{ cross_join_unnest('poke_pokemon', 'stats') }}
where stats is not null
-- stats at poke_pokemon/stats

