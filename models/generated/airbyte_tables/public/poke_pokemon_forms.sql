{{ config(schema="public", tags=["nested"]) }}
-- Final base SQL model
select
    _airbyte_poke_pokemon_hashid,
    url,
    {{ adapter.quote('name') }},
    _airbyte_emitted_at,
    _airbyte_forms_hashid
from {{ ref('poke_pokemon_forms_ab3') }}
-- forms at poke_pokemon/forms from {{ ref('poke_pokemon') }}

