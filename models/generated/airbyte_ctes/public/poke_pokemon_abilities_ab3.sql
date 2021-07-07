{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to build a hash column based on the values of this record
select
    *,
    {{ dbt_utils.surrogate_key([
        '_airbyte_poke_pokemon_hashid',
        'slot',
        'ability',
        boolean_to_string('is_hidden'),
    ]) }} as _airbyte_abilities_hashid
from {{ ref('poke_pokemon_abilities_ab2') }}
-- abilities at poke_pokemon/abilities

