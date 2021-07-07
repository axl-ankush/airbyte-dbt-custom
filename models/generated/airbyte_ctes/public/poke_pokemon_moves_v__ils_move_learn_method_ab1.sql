{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    _airbyte_version_group_details_hashid,
    {{ json_extract_scalar('move_learn_method', ['url']) }} as url,
    {{ json_extract_scalar('move_learn_method', ['name']) }} as {{ adapter.quote('name') }},
    _airbyte_emitted_at
from {{ ref('poke_pokemon_moves_version_group_details') }}
where move_learn_method is not null
-- move_learn_method at poke_pokemon/moves/version_group_details/move_learn_method

