{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
{{ unnest_cte('poke_pokemon', 'poke_pokemon', 'abilities') }}
select
    _airbyte_poke_pokemon_hashid,
    {{ json_extract_scalar(unnested_column_value('abilities'), ['slot']) }} as slot,
    {{ json_extract(unnested_column_value('abilities'), ['ability']) }} as ability,
    {{ json_extract_scalar(unnested_column_value('abilities'), ['is_hidden']) }} as is_hidden,
    _airbyte_emitted_at
from {{ ref('poke_pokemon') }}
{{ cross_join_unnest('poke_pokemon', 'abilities') }}
where abilities is not null
-- abilities at poke_pokemon/abilities

