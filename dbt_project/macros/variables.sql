
{% macro learn_variables() %}
    {% set your_name_jinja="Siva" %}
    {{ log("Hello "~your_name_jinja,info=True )}}
    {{ log("Hello User "~ var("user_name","No USERNAME IS SET!!") ~ "!", info=True ) }}

## dbt run-operation learn_variables 
{% endmacro %}



