{{ config(schema="public", tags=["nested"]) }}
-- Final base SQL model
select
    _airbyte_version_group_details_hashid,
    url,
    {{ adapter.quote('name') }},
    _airbyte_emitted_at,
    _airbyte_move_learn_method_hashid
from {{ ref('poke_pokemon_moves_v__ils_move_learn_method_ab3') }}
-- move_learn_method at poke_pokemon/moves/version_group_details/move_learn_method from {{ ref('poke_pokemon_moves_version_group_details') }}

