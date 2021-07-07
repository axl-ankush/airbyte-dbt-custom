{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to build a hash column based on the values of this record
select
    *,
    {{ dbt_utils.surrogate_key([
        '_airbyte_poke_pokemon_hashid',
        'item',
        array_to_string('version_details'),
    ]) }} as _airbyte_held_items_hashid
from {{ ref('poke_pokemon_held_items_ab2') }}
-- held_items at poke_pokemon/held_items

