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
        from ext_schema_datalake.meetime_calls ) 
select id,
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
 from cte
 where record=1
 order by id