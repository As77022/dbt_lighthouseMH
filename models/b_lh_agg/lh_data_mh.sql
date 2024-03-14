{{ 
  config(
    materialized='table',
    schema='stg_agg'
  ) 
}}

WITH SchoolData AS (
    SELECT
        SC.studentid AS student_id,
        SC.isactive AS student_status,
        ST.statename AS state,
        CASE
          WHEN S.isimplemented = 'true' THEN 'Started'
          WHEN S.isimplemented = 'false' THEN 'Not Started'
          ELSE NULL
        END AS school_status,
        SE.sectorname AS state_sector,
        CASE
          WHEN S.schooltypeid = '2' THEN 'Non-Composite (9-10)'
          WHEN S.schooltypeid = '187' THEN 'Composite (9-12)'
          WHEN S.schooltypeid = '1' THEN 'Composite (9-12)'
          WHEN S.schooltypeid = '188' THEN 'Non-Composite (9-10)'
          WHEN S.schooltypeid = '191' THEN 'Non-Composite (9-10)'
          WHEN S.schooltypeid = '189' THEN 'Composite (9-12)'
          ELSE S.schooltypeid
        END AS school_category,
        CASE
          WHEN S.schoolmanagementid = '182' THEN 'Government'
          WHEN S.schoolmanagementid = '194' THEN 'Government'
          WHEN S.schoolmanagementid = '183' THEN 'Government'
          WHEN S.schoolmanagementid = '185' THEN 'Government'
          WHEN S.schoolmanagementid = '404' THEN 'Government'
          WHEN S.schoolmanagementid = '192' THEN 'Government'
          WHEN S.schoolmanagementid = '198' THEN 'Government'
          ELSE S.schoolmanagementid
        END AS school_type,
        CASE
          WHEN TE.tereceivestatus::text = 'Yes' THEN 'Available'
          WHEN TE.tereceivestatus::text = 'No' THEN 'Not Available'
          ELSE TE.tereceivestatus
        END AS lab,
        S.udise AS school_id_udi,
        AY.yearname AS yearname,
        VT.fullname AS vt_name,
        CASE 
            WHEN VT.isactive::text = 'true' THEN 'Available'
            WHEN VT.isactive::text = 'false' THEN 'Not Available'
            ELSE NULL
        END AS vt_status,
        VT.isactive as vt_is_active,
        VTP.vtpname AS vtp,
        VTP.isactive AS vtp_status,
        class.classcode AS class,
        JR.jobrolename AS lahi_job_role,
        SUM(CASE WHEN CAST(C.gender AS INTEGER) = 207 THEN 1 ELSE 0 END) AS total_boys,
        SUM(CASE WHEN CAST(C.gender AS INTEGER) = 208 THEN 1 ELSE 0 END) AS total_girls
    FROM {{ source('lighthouseMH','schools') }} as S
    LEFT JOIN {{ source('lighthouseMH','states') }} ST ON S.statecode = ST.statecode
    LEFT JOIN {{ source('lighthouseMH', 'student_class_mapping') }} SC ON S.schoolid = SC.schoolid
    LEFT JOIN {{ source('lighthouseMH','student_classes') }} C ON SC.studentid = C.studentid
    LEFT JOIN {{ source('lighthouseMH','sections') }} sections ON SC.sectionid = sections.sectionid
    LEFT JOIN {{ source('lighthouseMH','school_classes') }} class ON SC.classid = class.classid
    LEFT JOIN {{ source('lighthouseMH','academic_years') }} AY ON SC.academicyearid = AY.academicyearid
    LEFT JOIN {{ source('lighthouseMH','vt_class_students') }} VCS ON SC.studentid = VCS.studentid
    LEFT JOIN {{ source('lighthouseMH','vt_school_sectors') }} VSS ON S.schoolid = VSS.schoolid
    LEFT JOIN {{ source('lighthouseMH','vocational_trainers') }} VT ON VSS.vtid = VT.vtid
    LEFT JOIN {{ source('lighthouseMH','schools_by_vtp_sector_info') }} SVTPS ON S.schoolid = SVTPS.implementedschoolid
    -- LEFT JOIN {{ ref('schools_by_vtp_sector_info') }} SVTPS ON S.schoolid = SVTPS.approvedschoolid
    LEFT JOIN {{ source('lighthouseMH','vocational_training_providers') }} VTP ON SVTPS.vtpid = VTP.vtpid
    LEFT JOIN {{ source('lighthouseMH','student_class_details') }} SCD ON SC.studentid = SCD.studentid
    LEFT JOIN {{ source('lighthouseMH','sectors') }} SE ON SCD.sectorid = SE.sectorid
    LEFT JOIN {{ source('lighthouseMH','job_roles') }} JR ON SCD.jobroleid = JR.jobroleid
    LEFT JOIN {{ source('lighthouseMH','tool_equipments') }} TE ON S.schoolid = TE.schoolid
    GROUP BY SC.studentid, ST.statename, S.isactive, SE.sectorname, TE.tereceivestatus, 
            S.udise, AY.yearname, VT.fullname, VT.isactive, JR.jobrolename, VTP.vtpname, 
            class.classcode, VTP.isactive, SC.isactive, S.isimplemented, VT.natureofappointment, 
            S.schooltypeid, S.schoolmanagementid
)

SELECT
    state,
    school_status::TEXT,
    state_sector,
    class,
    school_category,
    school_type,
    lab,
    school_id_udi,
    yearname as academic_year,
    vtp,
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
    FROM SchoolData SD
    WHERE state_sector IS NOT NULL AND lahi_job_role IS NOT NULL
) AS RankedData
WHERE row_num = 1