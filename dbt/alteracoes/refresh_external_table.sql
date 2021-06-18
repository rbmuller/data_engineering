{% macro redshift__refresh_external_table(source_node) %}

    {%- set partitions = source_node.external.get('partitions',[]) -%}

    {%- if partitions -%}
    
        {%- set part_len = partitions|length -%}
    
        {%- set get_partitions_sql -%}

        SELECT * FROM (

        select * from
        
        {%- for partition in partitions %} (
        
            {%- set part_num = loop.index -%}
            
            {%- if partition.vals.macro -%}
                {%- set vals = dbt_external_tables.render_from_context(partition.vals.macro, **partition.vals.args) -%}
            {%- elif partition.vals is string -%}
                {%- set vals = [partition.vals] -%}
            {%- else -%}
                {%- set vals = partition.vals -%}
            {%- endif -%}
        
            {%- for val in vals %}
            
                select
                    '{{ partition.name }}' as name_{{ part_num }},
                    '{{ val }}' as val_{{ part_num }},
                    '{{ dbt_external_tables.render_from_context(partition.path_macro, partition.name, val) }}' as path_{{ part_num }}
                
                {{ 'union all' if not loop.last else ') ' }}
            
            {%- endfor -%}

            {{ 'cross join' if not loop.last }}

        {%- endfor -%}
        )
        {{ ' AS D group by ' }}

        {%- for partition in partitions %}

            {%- set part_num = loop.index -%}
            D.name_{{ part_num }}{{ ', '}}
            D.path_{{ part_num }}{{ ', '}}
            D.val_{{ part_num }}{{ ', ' if not loop.last }}

        {%- endfor -%}

        {%- endset -%}
        
        {%- set finals = [] -%}
        
        {%- if execute -%}
            {%- set results = run_query(get_partitions_sql) -%}
            {%- for row in results -%}
                
                {%- set partition_parts = [] -%}
                {%- set path_parts = [] -%}
                
                {%- for i in range(0, part_len) -%}
                    {%- do partition_parts.append({
                        'name': row[i * 3],
                        'value': row[i * 3 + 1]
                    }) -%}
                    {%- do path_parts.append(row[i * 3 + 2]) -%}
                {%- endfor -%}
                
                {%- set construct = {
                    'partition_by': partition_parts,
                    'path': path_parts | join('/')
                }  -%}
                
                {% do finals.append(construct) %}
            {%- endfor -%}
        {%- endif -%}
    
        {%- set ddl = dbt_external_tables.redshift_alter_table_add_partitions(source_node, finals) -%}
        {{ return(ddl) }}
    
    {% else %}
    
        {% do return([]) %}
    
    {% endif %}
    
{% endmacro %}