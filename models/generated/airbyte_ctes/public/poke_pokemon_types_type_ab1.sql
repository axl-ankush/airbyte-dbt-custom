{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    _airbyte_types_hashid,
    {{ json_extract_scalar(adapter.quote('type'), ['url']) }} as url,
    {{ json_extract_scalar(adapter.quote('type'), ['name']) }} as {{ adapter.quote('name') }},
    _airbyte_emitted_at
from {{ ref('poke_pokemon_types') }}
where {{ adapter.quote('type') }} is not null
-- type at poke_pokemon/types/type

