{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    _airbyte_stats_hashid,
    {{ json_extract_scalar('stat', ['url']) }} as url,
    {{ json_extract_scalar('stat', ['name']) }} as {{ adapter.quote('name') }},
    _airbyte_emitted_at
from {{ ref('poke_pokemon_stats') }}
where stat is not null
-- stat at poke_pokemon/stats/stat

