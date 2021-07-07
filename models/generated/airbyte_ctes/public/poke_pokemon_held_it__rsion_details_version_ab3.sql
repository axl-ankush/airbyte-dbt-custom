{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to build a hash column based on the values of this record
select
    *,
    {{ dbt_utils.surrogate_key([
        '_airbyte_version_details_hashid',
        'url',
        adapter.quote('name'),
    ]) }} as _airbyte_version_hashid
from {{ ref('poke_pokemon_held_it__rsion_details_version_ab2') }}
-- version at poke_pokemon/held_items/version_details/version

