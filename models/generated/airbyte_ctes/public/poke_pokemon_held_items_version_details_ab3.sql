{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to build a hash column based on the values of this record
select
    *,
    {{ dbt_utils.surrogate_key([
        '_airbyte_held_items_hashid',
        'rarity',
        adapter.quote('version'),
    ]) }} as _airbyte_version_details_hashid
from {{ ref('poke_pokemon_held_items_version_details_ab2') }}
-- version_details at poke_pokemon/held_items/version_details

