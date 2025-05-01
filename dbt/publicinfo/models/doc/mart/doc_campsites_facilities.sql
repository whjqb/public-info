{{
    config(
        materialized='incremental',
        unique_key='asset_id',
        on_schema_change='sync_all_columns'
    )
}}

with source as
(
    select * from {{ ref('stg_doc_campsites_detail') }}
)

select 
    asset_id,
    facility,
    current_timestamp as loaded_at
from source,
lateral jsonb_array_elements_text(facilities) as facility

