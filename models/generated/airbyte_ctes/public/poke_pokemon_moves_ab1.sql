{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
{{ unnest_cte('poke_pokemon', 'poke_pokemon', 'moves') }}
select
    _airbyte_poke_pokemon_hashid,
    {{ json_extract(unnested_column_value('moves'), ['move']) }} as {{ adapter.quote('move') }},
    {{ json_extract_array(unnested_column_value('moves'), ['version_group_details']) }} as version_group_details,
    _airbyte_emitted_at
from {{ ref('poke_pokemon') }}
{{ cross_join_unnest('poke_pokemon', 'moves') }}
where moves is not null
-- moves at poke_pokemon/moves

