{{ config(schema="_airbyte_public", tags=["top-level-intermediate"]) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    {{ json_extract_scalar('_airbyte_data', ['id']) }} as {{ adapter.quote('id') }},
    {{ json_extract_scalar('_airbyte_data', ['name']) }} as {{ adapter.quote('name') }},
    {{ json_extract_array('_airbyte_data', ['forms']) }} as forms,
    {{ json_extract_array('_airbyte_data', ['moves']) }} as moves,
    {{ json_extract_scalar('_airbyte_data', ['order']) }} as {{ adapter.quote('order') }},
    {{ json_extract_array('_airbyte_data', ['stats']) }} as stats,
    {{ json_extract_array('_airbyte_data', ['types']) }} as {{ adapter.quote('types') }},
    {{ json_extract_scalar('_airbyte_data', ['height']) }} as height,
    {{ json_extract_scalar('_airbyte_data', ['weight']) }} as weight,
    {{ json_extract('_airbyte_data', ['species']) }} as species,
    {{ json_extract('_airbyte_data', ['sprites']) }} as sprites,
    {{ json_extract_array('_airbyte_data', ['abilities']) }} as abilities,
    {{ json_extract_array('_airbyte_data', ['held_items']) }} as held_items,
    {{ json_extract_scalar('_airbyte_data', ['is_default ']) }} as {{ adapter.quote('is_default ') }},
    {{ json_extract_array('_airbyte_data', ['game_indices']) }} as game_indices,
    {{ json_extract_scalar('_airbyte_data', ['base_experience']) }} as base_experience,
    {{ json_extract_scalar('_airbyte_data', ['location_area_encounters']) }} as location_area_encounters,
    _airbyte_emitted_at
from {{ source('public', '_airbyte_raw_poke_pokemon') }}
-- poke_pokemon

