{{ config(schema="public", tags=["nested"]) }}
-- Final base SQL model
select
    _airbyte_moves_hashid,
    url,
    {{ adapter.quote('name') }},
    _airbyte_emitted_at,
    _airbyte_move_hashid
from {{ ref('poke_pokemon_moves_move_ab3') }}
-- move at poke_pokemon/moves/move from {{ ref('poke_pokemon_moves') }}

