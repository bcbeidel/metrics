{{ config(materialized='ephemeral') }}

select * from {{ var('metrics__user_mapping_table') }}
