{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to build a hash column based on the values of this record
select
    *,
    {{ dbt_utils.surrogate_key([
        '_airbyte_version_group_details_hashid',
        'url',
        adapter.quote('name'),
    ]) }} as _airbyte_version_group_hashid
from {{ ref('poke_pokemon_moves_v__details_version_group_ab2') }}
-- version_group at poke_pokemon/moves/version_group_details/version_group

