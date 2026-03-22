with exploded as (
    select
        api.appid,
        trim(genre_name) as genre
    from {{ ref('stg_steam_api') }} api
    cross join unnest(split(coalesce(api.genre, ''), ',')) as genre_name
)

select distinct
    appid,
    genre
from exploded
where genre <> ''
  and lower(genre) <> 'free to play'
