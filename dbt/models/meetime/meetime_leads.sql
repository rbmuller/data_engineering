{{ config(materialized='table') }}

WITH SOURCE AS:
SELECT  id::int,
        current_prospection_id::int,
        external_reference,
        lead_annotations,
        lead_city,
        lead_company,        
        TO_TIMESTAMP(lead_created_date, 'YYYY-MM-DD HH24:MI:SS') at time zone 'America/Sao_Paulo' AS lead_created_date,
        TO_TIMESTAMP(lead_deleted_date, 'YYYY-MM-DD HH24:MI:SS') at time zone 'America/Sao_Paulo' AS lead_deleted_date,
        lead_email,
        lead_facebook,
        lead_linkedin,
        lead_name,
        lead_position,
        lead_site,
        lead_state,
        lead_twitter,
        public_url,
        list_phones.label as phone_label,
        list_phones.phone as phone_number,
        list_phones.lastUsage as phone_last_usage,
        segmento,
        b2bB2c,
        --list_tag,
        preVendedor,
        departamento,
        noFuncionario,
        recovered,
        origemDoProspect,
        reuniao,
        contatosAdicionais,
        contatosAdicionais2
FROM ext_schema_datalake.meetime_leads l
left join l.phones list_phones on TRUE
--WHERE  updated = (SELECT MAX(updated) FROM {{ source('meetime', 'meetime_calls') }} B WHERE B.ID=A.ID )
) 
SELECT *
FROM SOURCE 

