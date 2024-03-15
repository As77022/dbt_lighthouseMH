{{ 
  config(
    materialized='table',
    schema='stg_agg'
  ) 
}}

WITH SchoolData AS 
(
    SELECT
        SC."StudentId" AS "student_id",
		    SC."IsActive" as "student_status",
        ST."StateName" AS "state",
        CASE
			    WHEN S."IsImplemented" = 'true' THEN 'Started'
			    WHEN S."IsImplemented" = 'false' THEN 'Not Started'
          ELSE NULL
        END AS "school_status",
		    SE."SectorName" AS "state_sector",
        CASE
          WHEN S."SchoolTypeId" = '2' THEN 'Non-Composite (9-10)'
			    WHEN S."SchoolTypeId" = '187' THEN 'Composite (9-12)'
			    WHEN S."SchoolTypeId" = '1' THEN 'Composite (9-12)'
			    WHEN S."SchoolTypeId" = '188' THEN 'Non-Composite (9-10)'
			    WHEN S."SchoolTypeId" = '191' THEN 'Non-Composite (9-10)'
			    WHEN S."SchoolTypeId" = '189' THEN 'Composite (9-12)'
          ELSE S."SchoolTypeId"
        END AS "school_category",
        CASE
          WHEN S."SchoolManagementId" = '182' THEN 'Government'
			    WHEN S."SchoolManagementId" = '194' THEN 'Government'
          WHEN S."SchoolManagementId" = '183' THEN 'Government'
          WHEN S."SchoolManagementId" = '185' THEN 'Government'
          WHEN S."SchoolManagementId" = '404' THEN 'Government'
          WHEN S."SchoolManagementId" = '198' THEN 'Government'
          ELSE S."SchoolManagementId"
        END AS "school_type",
        S."UDISE" AS "school_id_udi",
        AY."YearName" AS "yearname",
        VT."FullName"  AS "vt_name",
        CASE 
            WHEN VT."IsActive"::text = 'true' THEN 'Available'
            WHEN VT."IsActive"::text = 'false' THEN 'Not Available'
            ELSE NULL
        END AS "vt_status",
        VT."IsActive" as "vt_is_active",
        class."ClassCode" AS "class",
        JR."JobRoleName" AS "lahi_job_role",
        SUM(CASE WHEN CAST(C."Gender" AS INTEGER) = 207 THEN 1 ELSE 0 END) AS "total_boys",
        SUM(CASE WHEN CAST(C."Gender" AS INTEGER) = 208 THEN 1 ELSE 0 END) AS "total_girls"
    FROM {{source('source_lahi_gj','schools')}} as S
    LEFT JOIN {{source('source_lahi_gj','states')}} AS ST ON S."StateCode" = ST."StateCode"
    LEFT JOIN {{source('source_lahi_gj','student_class_mapping')}} AS SC ON S."SchoolId" = SC."SchoolId"
    LEFT JOIN {{source('source_lahi_gj','student_classes')}} AS C ON SC."StudentId" = C."StudentId"
    LEFT JOIN {{source('source_lahi_gj','school_classes')}} AS class ON SC."ClassId" = class."ClassId"
    LEFT JOIN {{source('source_lahi_gj','academic_years')}} AS AY ON SC."AcademicYearId" = AY."AcademicYearId"
    LEFT JOIN {{source('source_lahi_gj','vt_class_students')}} AS VCS ON SC."StudentId" = VCS."StudentId"
    LEFT JOIN {{source('source_lahi_gj','vt_school_sectors')}} AS VSS ON S."SchoolId" = VSS."SchoolId"
    LEFT JOIN {{source('source_lahi_gj','vocational_trainers')}} AS VT ON VSS."VTId" = VT."VTId"
	LEFT JOIN {{source('source_lahi_gj','student_class_details')}} AS SCD ON SC."StudentId" = SCD."StudentId"
    LEFT JOIN {{source('source_lahi_gj','sectors')}} AS SE ON SCD."SectorId" = SE."SectorId"
    LEFT JOIN {{source('source_lahi_gj','job_roles')}} AS JR ON SCD."JobRoleId" = JR."JobRoleId"
    LEFT JOIN {{source('source_lahi_gj','tool_equipments')}} AS TE ON S."SchoolId" = TE."SchoolId"
    GROUP BY SC."StudentId", ST."StateName", S."IsActive", SE."SectorName",
            S."UDISE", AY."YearName", VT."FullName", VT."IsActive", JR."JobRoleName", 
            class."ClassCode", SC."IsActive", S."IsImplemented", VT."NatureOfAppointment", 
            S."SchoolTypeId", S."SchoolManagementId"
 )
SELECT
    state,
    school_status::TEXT,
    state_sector,
    class,
    school_category,
    school_type,
    school_id_udi,
    yearname as academic_year,
    total_boys,
    total_girls,
    total_boys + total_girls AS grand_total,
    vt_name,
    vt_status,
    lahi_job_role
FROM (
    SELECT
        SD.*,
        ROW_NUMBER() OVER (PARTITION BY school_id_udi, lahi_job_role, state_sector ORDER BY school_id_udi) AS row_num
    FROM SchoolData AS SD
    WHERE state_sector IS NOT NULL AND lahi_job_role IS NOT NULL
) AS RankedData
WHERE row_num = 1