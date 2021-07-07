{{ config(schema="public", tags=["nested"]) }}
-- Final base SQL model
select
    _airbyte_version_group_details_hashid,
    url,
    {{ adapter.quote('name') }},
    _airbyte_emitted_at,
    _airbyte_version_group_hashid
from {{ ref('poke_pokemon_moves_v__details_version_group_ab3') }}
-- version_group at poke_pokemon/moves/version_group_details/version_group from {{ ref('poke_pokemon_moves_version_group_details') }}

