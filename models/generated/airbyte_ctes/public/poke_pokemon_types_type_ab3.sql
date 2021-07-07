{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to build a hash column based on the values of this record
select
    *,
    {{ dbt_utils.surrogate_key([
        '_airbyte_types_hashid',
        'url',
        adapter.quote('name'),
    ]) }} as _airbyte_type_hashid
from {{ ref('poke_pokemon_types_type_ab2') }}
-- type at poke_pokemon/types/type

