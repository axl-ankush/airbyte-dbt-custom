{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to build a hash column based on the values of this record
select
    *,
    {{ dbt_utils.surrogate_key([
        '_airbyte_poke_pokemon_hashid',
        'url',
        adapter.quote('name'),
    ]) }} as _airbyte_forms_hashid
from {{ ref('poke_pokemon_forms_ab2') }}
-- forms at poke_pokemon/forms

