-- Raw landing -> typed staging. One row per arrival (dedup happens downstream).
with source as (
    select * from {{ source('stream', 'raw_ticks') }}
)
select
    symbol,
    cast(event_time as timestamp) as event_time,
    cast(price as double)         as price,
    cast(ingest_seq as bigint)    as ingest_seq,
    arrived_at
from source
