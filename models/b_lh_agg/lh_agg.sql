{{config(materialized = 'table',
            schema = 'stg_agg')}}


WITH mycte AS (
    SELECT  *
    FROM {{source('lighthouseMH','student_classes')}}
    UNION
    SELECT *
    FROM {{source('source_lahi_gj','student_classes')}}
)
SELECT *
FROM 
mycte
