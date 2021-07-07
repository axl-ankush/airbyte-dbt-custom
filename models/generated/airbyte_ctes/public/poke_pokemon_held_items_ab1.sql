{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
{{ unnest_cte('poke_pokemon', 'poke_pokemon', 'held_items') }}
select
    _airbyte_poke_pokemon_hashid,
    {{ json_extract(unnested_column_value('held_items'), ['item']) }} as item,
    {{ json_extract_array(unnested_column_value('held_items'), ['version_details']) }} as version_details,
    _airbyte_emitted_at
from {{ ref('poke_pokemon') }}
{{ cross_join_unnest('poke_pokemon', 'held_items') }}
where held_items is not null
-- held_items at poke_pokemon/held_items

