{{ config(schema="public", tags=["nested"]) }}
-- Final base SQL model
select
    _airbyte_poke_pokemon_hashid,
    url,
    {{ adapter.quote('name') }},
    _airbyte_emitted_at,
    _airbyte_species_hashid
from {{ ref('poke_pokemon_species_ab3') }}
-- species at poke_pokemon/species from {{ ref('poke_pokemon') }}

