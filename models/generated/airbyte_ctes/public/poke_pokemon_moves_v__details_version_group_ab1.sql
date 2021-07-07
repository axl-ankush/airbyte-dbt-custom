{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    _airbyte_version_group_details_hashid,
    {{ json_extract_scalar('version_group', ['url']) }} as url,
    {{ json_extract_scalar('version_group', ['name']) }} as {{ adapter.quote('name') }},
    _airbyte_emitted_at
from {{ ref('poke_pokemon_moves_version_group_details') }}
where version_group is not null
-- version_group at poke_pokemon/moves/version_group_details/version_group

