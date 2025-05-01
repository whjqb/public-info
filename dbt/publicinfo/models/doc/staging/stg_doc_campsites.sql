with source as 
(
    select * from {{ source('doc', 'doc_campsites') }}
)

select 
    cast(json_data ->> 'assetId' as int) as asset_id,
    json_data ->> 'name' as name,
    json_data ->> 'region' as region,
    json_data ->> 'status' as status,
    cast(json_data ->> 'x' as float) as easting,
    cast(json_data ->> 'y' as float) as northing,
    current_timestamp as loaded_at
from source,
lateral jsonb_array_elements(raw_data) as json_data