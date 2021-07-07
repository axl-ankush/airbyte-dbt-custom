{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to build a hash column based on the values of this record
select
    *,
    {{ dbt_utils.surrogate_key([
        '_airbyte_moves_hashid',
        'version_group',
        'level_learned_at',
        'move_learn_method',
    ]) }} as _airbyte_version_group_details_hashid
from {{ ref('poke_pokemon_moves_version_group_details_ab2') }}
-- version_group_details at poke_pokemon/moves/version_group_details

