{{ config(schema="public", tags=["nested"]) }}
-- Final base SQL model
select
    _airbyte_moves_hashid,
    version_group,
    level_learned_at,
    move_learn_method,
    _airbyte_emitted_at,
    _airbyte_version_group_details_hashid
from {{ ref('poke_pokemon_moves_version_group_details_ab3') }}
-- version_group_details at poke_pokemon/moves/version_group_details from {{ ref('poke_pokemon_moves') }}

