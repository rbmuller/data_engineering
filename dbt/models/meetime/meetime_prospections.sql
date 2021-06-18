{{ config(materialized='table') }}

with source as (
    SELECT
        id,
        lead_id,
        owner_id,
        owner_name,
        cadence,
        cadence_id,
        TO_TIMESTAMP(rd_conversion_date, 'YYYY-MM-DD HH24:MI:SS') at time zone 'America/Sao_Paulo' AS rd_conversion_date,
        TO_TIMESTAMP(created_date, 'YYYY-MM-DD HH24:MI:SS') at time zone 'America/Sao_Paulo' AS created_date,
        TO_TIMESTAMP(deleted_date, 'YYYY-MM-DD HH24:MI:SS') at time zone 'America/Sao_Paulo' AS deleted_date,
        TO_TIMESTAMP(begin_date, 'YYYY-MM-DD HH24:MI:SS') at time zone 'America/Sao_Paulo' AS begin_date,
        TO_TIMESTAMP(scheduled_date, 'YYYY-MM-DD HH24:MI:SS') at time zone 'America/Sao_Paulo' AS scheduled_date,
        TO_TIMESTAMP(end_date, 'YYYY-MM-DD HH24:MI:SS') at time zone 'America/Sao_Paulo' AS end_date,
        TO_TIMESTAMP(last_activity_date, 'YYYY-MM-DD HH24:MI:SS') at time zone 'America/Sao_Paulo' AS last_activity_date,
        status,
        lost_reason,
        lost_reason_id,
        lead_origin_channel,
        lead_origin_source,
        lead_origin_campaign,
        conversion
    FROM {{ source('meetime', 'meetime_prospections') }}
)
select *
from source