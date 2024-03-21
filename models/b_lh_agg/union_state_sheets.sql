{{ config(
  materialized='table'
) }}

with my_cte as (
        SELECT * FROM {{source('gsheet_data','andhra_pradesh_database')}} UNION
        SELECT * FROM {{source('gsheet_data','assam_database')}} UNION
        SELECT * FROM {{source('gsheet_data','chattisgarh_database')}} UNION
        SELECT * FROM {{source('gsheet_data','delhi_database')}} UNION
        SELECT * FROM {{source('gsheet_data','gujarat_database')}} UNION
        SELECT * FROM {{source('gsheet_data','himachal_pradesh_database')}} UNION
        SELECT * FROM {{source('gsheet_data','jharkhand_database')}} UNION
        SELECT * FROM {{source('gsheet_data','karnataka_database')}} UNION
        SELECT * FROM {{source('gsheet_data','ladakh_database')}} UNION
        SELECT * FROM {{source('gsheet_data','maharashtra_database')}} UNION
        SELECT * FROM {{source('gsheet_data','nagaland_database')}} UNION
        SELECT * FROM {{source('gsheet_data','odisha_database')}} UNION
        SELECT * FROM {{source('gsheet_data','rajasthan_database')}} UNION
        SELECT * FROM {{source('gsheet_data','tamil_nadu_database')}} UNION
        SELECT * FROM {{source('gsheet_data','uttarakhand_database')}} UNION
        SELECT * FROM {{source('gsheet_data','punjab_database')}} 
)

SELECT 
    "State",
    CASE
        WHEN "Academic_Year" = '2021-2022' THEN '2021-22'
        WHEN "Academic_Year" = '2022-2023' THEN '2022-23'
        WHEN "Academic_Year" = '2023-2024' THEN '2023-24'
        ELSE "Academic_Year"
    END AS Academic_Year,
    "School_Status",
    CASE
        WHEN "School_Type" = 'Provincialised' THEN 'Government'
        WHEN "School_Type" = 'Provincialized' THEN 'Government'
        WHEN "School_Type" = 'GOVT. PROVINCIALISED' THEN 'Government'
        WHEN "School_Type" = 'GOVT AIDED' THEN 'Government'
        WHEN "School_Type" = 'Govt.' THEN 'Government'
        WHEN "School_Type" = 'Department of Education' THEN 'Government'
        WHEN "School_Type" = 'Private Unaided' THEN 'Private - Unaided'
        WHEN "School_Type" = 'Government Aided' THEN 'Government'
        ELSE "School_Type"
    END AS "School_Type",
    CASE WHEN "Total_Boys" ~ '^[0-9\.]+$' THEN "Total_Boys"::numeric ELSE 0 END as Total_Boys,
    CASE WHEN "Total_Girls" ~ '^[0-9\.]+$' THEN "Total_Girls"::numeric ELSE 0 END as Total_Girls,
    "School_Category",
    "LAB",
    "School_ID___UDI",
    "VTP",
    CASE WHEN "Grand_Total" ~ '^[0-9\.]+$' THEN "Grand_Total"::numeric ELSE 0 END as Grand_Total,
    "VT_Name_",
    "VT_Status",
    "Job_Role__For_Class_11_and_12_",
    "Job_Role__For_Class_9_and_10_"
FROM my_cte

