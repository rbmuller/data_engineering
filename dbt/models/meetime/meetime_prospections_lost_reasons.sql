{{ config(materialized='table') }}

with source as (
    SELECT
        id,
        name
    FROM {{ source('meetime', 'meetime_prospections_lost_reasons') }}
)
select *
from source