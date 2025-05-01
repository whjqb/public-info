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
    name,
    place,
    region,
    status,
    bookable,
    static_link,
    introduction,
    location_string,
    campsite_category,
    number_of_powered_sites,
    number_of_unpowered_sites,
    easting,
    northing,
    current_timestamp as loaded_at
from source