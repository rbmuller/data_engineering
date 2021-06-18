{{ config(materialized='table') }}

select c.id, 
       c.name, 
       to_timestamp(created, 'YYYY-MM-DD HH24:MI:SS') at time zone 'America/Sao_Paulo' AS created_at,
       c.status,
       list_modules as modules,
       list_integrations as integrations,
       list_add_ons as add_ons
from ext_schema_datalake.meetime_company c 
left join c.modules list_modules on true
left join c.integrations list_integrations on true
left join c.add_ons list_add_ons on true
group by 1,2,3,4,5,6,7


-- The original sintax from DBT, but not considering JSON nested data

--{{ config(materialized='table',) }}

--with source as (
  --  SELECT
	  --id,
      --name,
      --TO_TIMESTAMP(created, 'YYYY-MM-DD HH24:MI:SS') created_at,
      --status,
      --modules,
      --integrations,
      --add_ons
--FROM {{ source('meetime', 'meetime_company') }}
--)
--select *
--from source