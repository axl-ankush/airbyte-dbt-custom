{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
{{ unnest_cte('poke_pokemon', 'poke_pokemon', adapter.quote('types')) }}
select
    _airbyte_poke_pokemon_hashid,
    {{ json_extract_scalar(unnested_column_value(adapter.quote('types')), ['slot']) }} as slot,
    {{ json_extract(unnested_column_value(adapter.quote('types')), ['type']) }} as {{ adapter.quote('type') }},
    _airbyte_emitted_at
from {{ ref('poke_pokemon') }}
{{ cross_join_unnest('poke_pokemon', adapter.quote('types')) }}
where {{ adapter.quote('types') }} is not null
-- types at poke_pokemon/types

