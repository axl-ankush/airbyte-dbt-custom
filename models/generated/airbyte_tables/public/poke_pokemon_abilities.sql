{{ config(schema="public", tags=["nested"]) }}
-- Final base SQL model
select
    _airbyte_poke_pokemon_hashid,
    slot,
    ability,
    is_hidden,
    _airbyte_emitted_at,
    _airbyte_abilities_hashid
from {{ ref('poke_pokemon_abilities_ab3') }}
-- abilities at poke_pokemon/abilities from {{ ref('poke_pokemon') }}

