{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    _airbyte_poke_pokemon_hashid,
    {{ json_extract_scalar('sprites', ['back_shiny']) }} as back_shiny,
    {{ json_extract_scalar('sprites', ['back_female']) }} as back_female,
    {{ json_extract_scalar('sprites', ['front_shiny']) }} as front_shiny,
    {{ json_extract_scalar('sprites', ['back_default']) }} as back_default,
    {{ json_extract_scalar('sprites', ['front_female']) }} as front_female,
    {{ json_extract_scalar('sprites', ['front_default']) }} as front_default,
    {{ json_extract_scalar('sprites', ['back_shiny_female']) }} as back_shiny_female,
    {{ json_extract_scalar('sprites', ['front_shiny_female']) }} as front_shiny_female,
    _airbyte_emitted_at
from {{ ref('poke_pokemon') }}
where sprites is not null
-- sprites at poke_pokemon/sprites

