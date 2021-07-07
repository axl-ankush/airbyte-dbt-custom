{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    _airbyte_poke_pokemon_hashid,
    {{ json_extract_scalar('species', ['url']) }} as url,
    {{ json_extract_scalar('species', ['name']) }} as {{ adapter.quote('name') }},
    _airbyte_emitted_at
from {{ ref('poke_pokemon') }}
where species is not null
-- species at poke_pokemon/species

