{{ config(schema="_airbyte_public", tags=["top-level-intermediate"]) }}
-- SQL model to build a hash column based on the values of this record
select
    *,
    {{ dbt_utils.surrogate_key([
        adapter.quote('id'),
        adapter.quote('name'),
        array_to_string('forms'),
        array_to_string('moves'),
        adapter.quote('order'),
        array_to_string('stats'),
        array_to_string(adapter.quote('types')),
        'height',
        'weight',
        'species',
        'sprites',
        array_to_string('abilities'),
        array_to_string('held_items'),
        boolean_to_string(adapter.quote('is_default ')),
        array_to_string('game_indices'),
        'base_experience',
        'location_area_encounters',
    ]) }} as _airbyte_poke_pokemon_hashid
from {{ ref('poke_pokemon_ab2') }}
-- poke_pokemon

