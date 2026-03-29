with exploded as (
    select
        api.appid,
        trim(genre_name) as genre
    from {{ ref('stg_steam_api') }} api
    cross join unnest(split(coalesce(api.genre, ''), ',')) as genre_name
)
# Filter out empty genres and the "Free To Play" genre, which is not really a genre but more of a business model
select distinct
    appid,
    genre
from exploded
where genre <> ''
  and lower(genre) <> 'free to play'
