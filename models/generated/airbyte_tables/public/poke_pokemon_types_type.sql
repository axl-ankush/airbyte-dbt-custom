{{ config(schema="public", tags=["nested"]) }}
-- Final base SQL model
select
    _airbyte_types_hashid,
    url,
    {{ adapter.quote('name') }},
    _airbyte_emitted_at,
    _airbyte_type_hashid
from {{ ref('poke_pokemon_types_type_ab3') }}
-- type at poke_pokemon/types/type from {{ ref('poke_pokemon_types') }}

