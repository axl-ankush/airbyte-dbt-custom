{{ config(schema="public", tags=["nested"]) }}
-- Final base SQL model
select
    _airbyte_poke_pokemon_hashid,
    {{ adapter.quote('move') }},
    version_group_details,
    _airbyte_emitted_at,
    _airbyte_moves_hashid
from {{ ref('poke_pokemon_moves_ab3') }}
-- moves at poke_pokemon/moves from {{ ref('poke_pokemon') }}

