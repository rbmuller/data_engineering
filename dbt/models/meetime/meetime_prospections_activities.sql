{{ config(materialized='table') }}

with cte as (
	select row_number() 
           over (partition by id order by updated desc) as record,
           *
        from ext_schema_datalake.meetime_prospections_activities ) 
select id,
       lead_id,
       name,
       TO_TIMESTAMP(updated, 'YYYY-MM-DD HH24:MI:SS') at time zone 'America/Sao_Paulo' AS updated,
       prospection_id,    
       activity_annotation,
       assigned_to_id,
       available_from,
       cadence,
       cadence_id,
       call_id,
       deleted_date,
       executed_by_id,
       TO_TIMESTAMP(execution_date, 'YYYY-MM-DD HH24:MI:SS') at time zone 'America/Sao_Paulo' AS execution_date,
	   TO_TIMESTAMP(goal_date, 'YYYY-MM-DD HH24:MI:SS') at time zone 'America/Sao_Paulo' AS goal_date,
       scope,
       status,
       type
 from cte
 where record=1
 order by id