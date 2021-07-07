{{ config(schema="_airbyte_public", tags=["nested-intermediate"]) }}
-- SQL model to build a hash column based on the values of this record
select
    *,
    {{ dbt_utils.surrogate_key([
        '_airbyte_held_items_hashid',
        'url',
        adapter.quote('name'),
    ]) }} as _airbyte_item_hashid
from {{ ref('poke_pokemon_held_items_item_ab2') }}
-- item at poke_pokemon/held_items/item

