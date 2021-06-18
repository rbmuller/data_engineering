{{ config(materialized='table') }}

with source as (
    SELECT
        id,
        field_key,
        name,
        data_type
        --options
    FROM {{ source('meetime', 'meetime_leads_custom_fields') }}
)
select *
from source