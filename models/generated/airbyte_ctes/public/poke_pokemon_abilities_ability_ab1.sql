{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    _airbyte_abilities_hashid,
    {{ json_extract_scalar('ability', ['url']) }} as url,
    {{ json_extract_scalar('ability', ['name']) }} as {{ adapter.quote('name') }},
    _airbyte_emitted_at
from {{ ref('poke_pokemon_abilities') }}
where ability is not null
-- ability at poke_pokemon/abilities/ability

