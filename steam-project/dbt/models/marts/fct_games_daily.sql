{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['appid', 'snapshot_date'],
    partition_by={"field": "snapshot_date", "data_type": "date"},
    cluster_by=['appid']
  )
}}

with api_source as (
    select
        cast(appid as int64) as appid,
        cast(price_usd as float64) as price_usd,
        cast(price_pln as float64) as price_pln,
        safe_cast(release_date as date) as release_date,
        cast(release_date_missing as bool) as release_date_missing,
        languages,
        genre,
        safe_cast(regexp_extract(_file_name, r'([0-9]{4}-[0-9]{2}-[0-9]{2})') as date) as snapshot_date,
        row_number() over (
            partition by cast(appid as int64), safe_cast(regexp_extract(_file_name, r'([0-9]{4}-[0-9]{2}-[0-9]{2})') as date)
            order by _file_name desc
        ) as rn
    from {{ source('silver_external', 'steam_api_raw') }}
    where appid is not null
),

genre_priority as (
    select
        lower(trim(cast(genre as string))) as genre,
        cast(priority as int64) as priority
    from {{ ref('genre_priority_seed') }}
),

api_daily as (
    select
        appid,
        price_usd,
        price_pln,
        release_date,
        release_date_missing,
        (
            select count(1)
            from unnest(split(coalesce(cast(languages as string), ''), ',')) as lang
            where trim(lang) != ''
        ) as languages,
        (
            select count(1)
            from unnest(split(coalesce(cast(genre as string), ''), ',')) as g
            where trim(g) != ''
        ) as genre,
        cast(genre as string) as genre_raw,
        snapshot_date
    from api_source
    where rn = 1
      and snapshot_date is not null
),

api_primary_genre as (
        select
                api.appid,
                api.snapshot_date,
                coalesce(
                        array_agg(
                                coalesce(gp.genre, lower(trim(g))) ignore nulls
                                order by coalesce(gp.priority, 999), coalesce(gp.genre, lower(trim(g)))
                                limit 1
                        )[safe_offset(0)],
                        'other'
                ) as primary_genre
        from api_daily api
        left join unnest(split(coalesce(api.genre_raw, ''), ',')) as g
            on trim(g) != ''
        left join genre_priority gp
            on gp.genre = lower(trim(g))
        group by api.appid, api.snapshot_date
),

spy_source as (
    select
        cast(appid as int64) as appid,
        cast(name as string) as title,
        cast(positive as int64) as positive_reviews,
        cast(negative as int64) as negative_reviews,
        cast(ccu as int64) as ccu,
        owners_category,
        coalesce(
            safe_cast(ingestion_date as date),
            safe_cast(regexp_extract(_file_name, r'([0-9]{4}-[0-9]{2}-[0-9]{2})') as date)
        ) as snapshot_date,
        row_number() over (
            partition by cast(appid as int64), coalesce(
                safe_cast(ingestion_date as date),
                safe_cast(regexp_extract(_file_name, r'([0-9]{4}-[0-9]{2}-[0-9]{2})') as date)
            )
            order by _file_name desc
        ) as rn
    from {{ source('silver_external', 'steamspy_raw') }}
    where appid is not null
),

spy_daily as (
    select
        appid,
        title,
        positive_reviews,
        negative_reviews,
        ccu,
        owners_category,
        snapshot_date
    from spy_source
    where rn = 1
      and snapshot_date is not null
),

joined as (
    select
        api.appid,
        api.snapshot_date,
        coalesce(spy.title, cast(api.appid as string)) as title,
        api.release_date,
        api.release_date_missing,
        api.price_usd,
        api.price_pln,
        api.languages,
        api.genre,
        coalesce(apg.primary_genre, 'other') as primary_genre,
        spy.owners_category,
        coalesce(spy.ccu, 0) as ccu,
        coalesce(spy.positive_reviews, 0) as positive_reviews,
        coalesce(spy.negative_reviews, 0) as negative_reviews,
        coalesce(spy.positive_reviews, 0) + coalesce(spy.negative_reviews, 0) as total_reviews,
        case
            when coalesce(spy.positive_reviews, 0) + coalesce(spy.negative_reviews, 0) = 0 then null
            else round(
                1.0 * coalesce(spy.positive_reviews, 0)
                / (coalesce(spy.positive_reviews, 0) + coalesce(spy.negative_reviews, 0)),
                4
            )
        end as positive_ratio
    from api_daily api
        left join api_primary_genre apg
            on api.appid = apg.appid
         and api.snapshot_date = apg.snapshot_date
    left join spy_daily spy
      on api.appid = spy.appid
     and api.snapshot_date = spy.snapshot_date
)

select *
from joined

{% if is_incremental() %}
where snapshot_date > (select coalesce(max(snapshot_date), date('1900-01-01')) from {{ this }})
{% endif %}
