{{ config(schema="public", tags=["nested"]) }}
-- Final base SQL model
select
    _airbyte_abilities_hashid,
    url,
    {{ adapter.quote('name') }},
    _airbyte_emitted_at,
    _airbyte_ability_hashid
from {{ ref('poke_pokemon_abilities_ability_ab3') }}
-- ability at poke_pokemon/abilities/ability from {{ ref('poke_pokemon_abilities') }}

