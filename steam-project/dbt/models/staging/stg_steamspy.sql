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
        cast(average_forever as float64) as average_forever,
        cast(average_2weeks as float64) as average_2weeks,
        cast(median_forever as float64) as median_forever,
        cast(median_2weeks as float64) as median_2weeks,
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
    average_forever,
    average_2weeks,
    median_forever,
    median_2weeks,
    ingestion_date
from ranked
where rn = 1
