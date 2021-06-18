{{ config(materialized='table') }}

with source as (
    SELECT cadence_focus,
        TO_TIMESTAMP(created_at, 'YYYY-MM-DD HH24:MI:SS') at time zone 'America/Sao_Paulo' AS created_at,
        created_by_id,
        TO_TIMESTAMP(deleted, 'YYYY-MM-DD HH24:MI:SS') at time zone 'America/Sao_Paulo' AS deleted,
        description,
        executing,
        id,
        name,
        owner_id,
        type
    FROM {{ source('meetime', 'meetime_cadences') }}
)
select *
from source
