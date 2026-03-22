with source as (
    select *
    from {{ source('silver_external', 'steam_api_raw') }}
),

ranked as (
    select
        cast(appid as int64) as appid,
        cast(price_usd as float64) as price_usd,
        cast(price_pln as float64) as price_pln,
        safe_cast(release_date as date) as release_date,
        cast(languages as string) as languages,
        cast(genre as string) as genre,
        cast(release_date_missing as bool) as release_date_missing,
        row_number() over (
            partition by cast(appid as int64)
            order by safe_cast(release_date as date) desc nulls last
        ) as rn
    from source
    where appid is not null
)

select
    appid,
    price_usd,
    price_pln,
    release_date,
    release_date_missing,
    languages,
    genre
from ranked
where rn = 1
