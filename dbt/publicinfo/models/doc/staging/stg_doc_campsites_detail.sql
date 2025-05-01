with source as 
(
    select * from {{ source('doc', 'doc_campsites_detail') }}
)

select
    cast(raw_data ->> 'assetId' as int) as asset_id,
    raw_data ->> 'name' as name,
    raw_data ->> 'place' as place,
    cast(raw_data ->> 'access' as jsonb) as access,
    raw_data ->> 'region' as region,
    raw_data ->> 'status' as status,
    raw_data ->> 'bookable' as bookable,
    cast(raw_data ->> 'landscape' as jsonb) as landscape,
    cast(raw_data ->> 'activities' as jsonb) as activities,
    cast(raw_data ->> 'facilities' as jsonb) as facilities,
    raw_data ->> 'staticLink' as static_link,
    raw_data ->> 'introduction' as introduction,
    raw_data ->> 'locationString' as location_string,
    raw_data ->> 'campsiteCategory' as campsite_category,
    cast(raw_data ->> 'numberOfPoweredSites' as int) as number_of_powered_sites,
    cast(raw_data ->> 'numberOfUnpoweredSites' as int) as number_of_unpowered_sites,
    cast(raw_data ->> 'x' as float) as easting,
    cast(raw_data ->> 'y' as float) as northing, 
    current_timestamp as loaded_at
from source
