select distinct
    to_hex(md5(cast(appid as string) || '|' || lower(language))) as language_key,
    appid,
    language,
    has_audio_support
from {{ ref('stg_languages') }}
# We can use the same logic as genres to filter out empty languages and any entries that are not really languages (e.g. "Audio in English", "Interface in Spanish", etc.). We will keep only the actual language names.