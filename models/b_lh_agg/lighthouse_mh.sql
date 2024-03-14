{{ config(
    materialized='table',
    schema='stg_agg'
) }}
with source_data as (
select 
        sc."FullName" as student_name,
        sc."Mobile" as student_mobile,
        ay."YearName" as academic_years
    FROM {{source('lighthouseMH','academic_years')}} as ay
    inner join {{ source('lighthouseMH', 'student_classes') }} as sc 
    on sc."AcademicYearId" = ay."AcademicYearId" -- Ensure the column name is correct
)
select *
from source_data
