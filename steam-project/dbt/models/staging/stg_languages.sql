with cleaned as (
  select
    cast(appid as int64) as appid,
    trim(
      regexp_replace(
        regexp_replace(cast(language as string), r'<[^>]+>', ''),
        r'(?i)languages with full audio support',
        ''
      )
    ) as language,
    coalesce(cast(audio as bool), false) as has_audio_support
  from {{ source('silver_external', 'languages_normalized') }}
)

select distinct
  appid,
  language,
  has_audio_support
from cleaned
where appid is not null
  and language is not null
  and language <> ''
  and lower(language) <> 'languages with full audio support'
