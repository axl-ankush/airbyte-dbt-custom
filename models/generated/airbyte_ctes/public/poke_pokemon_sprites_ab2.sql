{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _airbyte_poke_pokemon_hashid,
    cast(back_shiny as {{ dbt_utils.type_string() }}) as back_shiny,
    cast(back_female as {{ dbt_utils.type_string() }}) as back_female,
    cast(front_shiny as {{ dbt_utils.type_string() }}) as front_shiny,
    cast(back_default as {{ dbt_utils.type_string() }}) as back_default,
    cast(front_female as {{ dbt_utils.type_string() }}) as front_female,
    cast(front_default as {{ dbt_utils.type_string() }}) as front_default,
    cast(back_shiny_female as {{ dbt_utils.type_string() }}) as back_shiny_female,
    cast(front_shiny_female as {{ dbt_utils.type_string() }}) as front_shiny_female,
    _airbyte_emitted_at
from {{ ref('poke_pokemon_sprites_ab1') }}
-- sprites at poke_pokemon/sprites

