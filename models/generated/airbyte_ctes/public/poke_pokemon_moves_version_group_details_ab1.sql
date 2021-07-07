{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
{{ unnest_cte('poke_pokemon_moves', 'moves', 'version_group_details') }}
select
    _airbyte_moves_hashid,
    {{ json_extract(unnested_column_value('version_group_details'), ['version_group']) }} as version_group,
    {{ json_extract_scalar(unnested_column_value('version_group_details'), ['level_learned_at']) }} as level_learned_at,
    {{ json_extract(unnested_column_value('version_group_details'), ['move_learn_method']) }} as move_learn_method,
    _airbyte_emitted_at
from {{ ref('poke_pokemon_moves') }}
{{ cross_join_unnest('moves', 'version_group_details') }}
where version_group_details is not null
-- version_group_details at poke_pokemon/moves/version_group_details

