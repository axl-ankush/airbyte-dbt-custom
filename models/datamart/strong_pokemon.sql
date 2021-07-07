select
    a.name,
    b.base_stat status_value,
    c.name status_name
from {{ ref('poke_pokemon') }} a
left join {{ ref('poke_pokemon_stats') }} b
  on a._airbyte_poke_pokemon_hashid = b._airbyte_poke_pokemon_hashid
left join {{ ref('poke_pokemon_stats_stat') }} c
   on b._airbyte_stats_hashid = c._airbyte_stats_hashid
where c.name = 'attack'
order by status_value desc