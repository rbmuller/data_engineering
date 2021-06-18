{{ config(materialized='table') }}

SELECT  id,
        name,
        email,
        team_id,
        team_name,
        role,
        list_modules,
        active,
        to_timestamp(deleted_on, 'YYYY-MM-DD HH24:MI:SS') at time zone 'America/Sao_Paulo' AS deleted_on
FROM ext_schema_datalake.meetime_users u
left join u.modules list_modules on true
