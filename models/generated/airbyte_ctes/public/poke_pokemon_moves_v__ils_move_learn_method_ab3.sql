{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to build a hash column based on the values of this record
select
    *,
    {{ dbt_utils.surrogate_key([
        '_airbyte_version_group_details_hashid',
        'url',
        adapter.quote('name'),
    ]) }} as _airbyte_move_learn_method_hashid
from {{ ref('poke_pokemon_moves_v__ils_move_learn_method_ab2') }}
-- move_learn_method at poke_pokemon/moves/version_group_details/move_learn_method

