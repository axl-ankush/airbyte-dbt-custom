{{ config(schema="public", tags=["nested"]) }}
-- Final base SQL model
select
    _airbyte_poke_pokemon_hashid,
    back_shiny,
    back_female,
    front_shiny,
    back_default,
    front_female,
    front_default,
    back_shiny_female,
    front_shiny_female,
    _airbyte_emitted_at,
    _airbyte_sprites_hashid
from {{ ref('poke_pokemon_sprites_ab3') }}
-- sprites at poke_pokemon/sprites from {{ ref('poke_pokemon') }}

