select distinct
    to_hex(md5(cast(appid as string) || '|' || lower(language))) as language_key,
    appid,
    language,
    has_audio_support
from {{ ref('stg_languages') }}
