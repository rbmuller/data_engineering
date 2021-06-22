{{ 
	config(
		materialized='incremental',
        unique_key='updated'
    )
}}

with cte as (
	select row_number() 
           over (partition by id order by updated desc) as record,
           *
        from ext_schema_datalake.meetime_demos) 
select id, 
        user_name,
        user_id,
        team_name,
        demo_title,
        important,
        email_invites,
        demo_description,
        demo_type,
        scheduled_by,
        TO_TIMESTAMP(schedule_start_date, 'YYYY-MM-DD HH24:MI:SS') at time zone 'America/Sao_Paulo' AS schedule_start_date,
        TO_TIMESTAMP(start_date, 'YYYY-MM-DD HH24:MI:SS') at time zone 'America/Sao_Paulo' AS start_date,
        scheduled_duration,
        duration_minutes,
        status,
        rating,
        demo_link,
        TO_TIMESTAMP(updated, 'YYYY-MM-DD HH24:MI:SS') at time zone 'America/Sao_Paulo' AS updated
 from cte
 where record=1
 order by id