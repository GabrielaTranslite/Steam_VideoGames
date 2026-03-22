with source as (
    select *
    from {{ source('silver_external', 'steamspy_raw') }}
),

ranked as (
    select
        cast(appid as int64) as appid,
        cast(name as string) as title,
        cast(positive as int64) as positive_reviews,
        cast(negative as int64) as negative_reviews,
        cast(ccu as int64) as ccu,
        owners_category,
        safe_cast(ingestion_date as date) as ingestion_date,
        row_number() over (
            partition by cast(appid as int64)
            order by safe_cast(ingestion_date as date) desc nulls last
        ) as rn
    from source
    where appid is not null
)

select
    appid,
    title,
    positive_reviews,
    negative_reviews,
    ccu,
    owners_category,
    ingestion_date
from ranked
where rn = 1
