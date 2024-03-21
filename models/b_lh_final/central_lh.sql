{{config(materialized = 'table')}}

with mycte AS
(
    SELECT
        *, cast('LH_MH' as varchar(10)) as data_source
    FROM {{ref('lh_data_mh')}}
    UNION
    SELECT
        *,cast('LH_GJ' as varchar(10)) as data_source
    FROM {{ref('lh_data_gj')}}
)
SELECT * FROM mycte