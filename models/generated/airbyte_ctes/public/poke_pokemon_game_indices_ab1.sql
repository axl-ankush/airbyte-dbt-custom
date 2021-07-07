{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
{{ unnest_cte('poke_pokemon', 'poke_pokemon', 'game_indices') }}
select
    _airbyte_poke_pokemon_hashid,
    {{ json_extract(unnested_column_value('game_indices'), ['version']) }} as {{ adapter.quote('version') }},
    {{ json_extract_scalar(unnested_column_value('game_indices'), ['game_index']) }} as game_index,
    _airbyte_emitted_at
from {{ ref('poke_pokemon') }}
{{ cross_join_unnest('poke_pokemon', 'game_indices') }}
where game_indices is not null
-- game_indices at poke_pokemon/game_indices

