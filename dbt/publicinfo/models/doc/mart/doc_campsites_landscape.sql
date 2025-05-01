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
    landscape_element as landscape,
    current_timestamp as loaded_at
from source,
lateral jsonb_array_elements_text(landscape) as landscape_element

