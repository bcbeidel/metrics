{{ config(materialized='view') }}

select * from {{ var('metrics__user_mapping_table') }}
