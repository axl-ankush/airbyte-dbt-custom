{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    _airbyte_moves_hashid,
    {{ json_extract_scalar(adapter.quote('move'), ['url']) }} as url,
    {{ json_extract_scalar(adapter.quote('move'), ['name']) }} as {{ adapter.quote('name') }},
    _airbyte_emitted_at
from {{ ref('poke_pokemon_moves') }}
where {{ adapter.quote('move') }} is not null
-- move at poke_pokemon/moves/move

