{{
    config(
        materialized='incremental',
        unique_key='asset_id',
        on_schema_change='sync_all_columns'
    )
}}

with source as
(
    select * from {{ ref('stg_doc_campsites_alerts') }}
)

select 
    asset_id,
    name,
    display_date,
    heading,
    detail,
    current_timestamp as loaded_at
from source
