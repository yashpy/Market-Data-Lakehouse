-- Incremental, idempotent fact table: the latest price per (symbol, event_time).
--
-- unique_key = ['symbol','event_time'] -> dbt emits a MERGE, so re-runs and re-delivered
-- events converge instead of duplicating. We keep only the max ingest_seq per key, so a
-- late (out-of-order) event can never overwrite a newer one.

{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['symbol', 'event_time'],
    on_schema_change='append_new_columns'
  )
}}

with ranked as (
    select
        symbol,
        event_time,
        price,
        ingest_seq,
        row_number() over (
            partition by symbol, event_time
            order by ingest_seq desc
        ) as rn
    from {{ ref('stg_ticks') }}

    {% if is_incremental() %}
      -- Only reprocess the recently-arrived slice (watermark on arrival time).
      where arrived_at > (select coalesce(max(arrived_at), '1900-01-01') from {{ this }})
    {% endif %}
)

select symbol, event_time, price, ingest_seq, current_timestamp as arrived_at
from ranked
where rn = 1
