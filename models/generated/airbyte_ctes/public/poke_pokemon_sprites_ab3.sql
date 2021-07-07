{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to build a hash column based on the values of this record
select
    *,
    {{ dbt_utils.surrogate_key([
        '_airbyte_poke_pokemon_hashid',
        'back_shiny',
        'back_female',
        'front_shiny',
        'back_default',
        'front_female',
        'front_default',
        'back_shiny_female',
        'front_shiny_female',
    ]) }} as _airbyte_sprites_hashid
from {{ ref('poke_pokemon_sprites_ab2') }}
-- sprites at poke_pokemon/sprites

