{{ config(schema="public", tags=["nested"]) }}
-- Final base SQL model
select
    _airbyte_poke_pokemon_hashid,
    slot,
    {{ adapter.quote('type') }},
    _airbyte_emitted_at,
    _airbyte_types_hashid
from {{ ref('poke_pokemon_types_ab3') }}
-- types at poke_pokemon/types from {{ ref('poke_pokemon') }}

