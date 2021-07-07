{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    _airbyte_version_details_hashid,
    {{ json_extract_scalar(adapter.quote('version'), ['url']) }} as url,
    {{ json_extract_scalar(adapter.quote('version'), ['name']) }} as {{ adapter.quote('name') }},
    _airbyte_emitted_at
from {{ ref('poke_pokemon_held_items_version_details') }}
where {{ adapter.quote('version') }} is not null
-- version at poke_pokemon/held_items/version_details/version

