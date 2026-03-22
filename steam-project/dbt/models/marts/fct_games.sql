select
    api.appid,
    coalesce(spy.title, cast(api.appid as string)) as title,
    api.release_date,
    api.release_date_missing,
    api.price_usd,
    api.price_pln,
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
from {{ ref('stg_steam_api') }} api
left join {{ ref('stg_steamspy') }} spy
    on api.appid = spy.appid
