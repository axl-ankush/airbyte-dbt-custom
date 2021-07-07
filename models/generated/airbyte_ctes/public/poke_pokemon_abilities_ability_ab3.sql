{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to build a hash column based on the values of this record
select
    *,
    {{ dbt_utils.surrogate_key([
        '_airbyte_abilities_hashid',
        'url',
        adapter.quote('name'),
    ]) }} as _airbyte_ability_hashid
from {{ ref('poke_pokemon_abilities_ability_ab2') }}
-- ability at poke_pokemon/abilities/ability

