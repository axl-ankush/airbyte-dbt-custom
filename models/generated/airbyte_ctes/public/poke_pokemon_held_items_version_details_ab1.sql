{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
{{ unnest_cte('poke_pokemon_held_items', 'held_items', 'version_details') }}
select
    _airbyte_held_items_hashid,
    {{ json_extract_scalar(unnested_column_value('version_details'), ['rarity']) }} as rarity,
    {{ json_extract(unnested_column_value('version_details'), ['version']) }} as {{ adapter.quote('version') }},
    _airbyte_emitted_at
from {{ ref('poke_pokemon_held_items') }}
{{ cross_join_unnest('held_items', 'version_details') }}
where version_details is not null
-- version_details at poke_pokemon/held_items/version_details

