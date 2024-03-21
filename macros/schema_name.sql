{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}

       {%- if target.schema != "dev_prod" -%}
            {% if node.fqn[1:-1]|length == 0 %}
                 {{target.schema}}_{{ default_schema }}    
            {% else %}
                {{target.schema}}_{{ node.fqn[1:-1] }}
            {% endif %}

       {% else %} 
            {% if node.fqn[1:-1]|length == 0 %}
                {{ default_schema }}    
            {% else %}
                {{ node.fqn[1:-1] }}
            {% endif %}
         {% endif %}
    {%- else -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
