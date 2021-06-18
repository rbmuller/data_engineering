{{ config(materialized='table') }}

with source as (
    SELECT
        id,
 		updated,
        user_id,
        user_name,
        call_type,
        connected_duration_seconds,
        TO_TIMESTAMP(date, 'YYYY-MM-DD HH24:MI:SS') at time zone 'America/Sao_Paulo' AS date,
        important,
        notes,
        origin_phone,
        origin_type,
        output,
        prince,
        receiver_phone,
        receiver_type,
        started_at,
        status,
        call_link
    FROM {{ source('meetime', 'meetime_calls') }} A
    WHERE  updated = (SELECT MAX(updated) FROM {{ source('meetime', 'meetime_calls') }} B WHERE B.ID=A.ID )
  	order by 1
)

select *
from source


