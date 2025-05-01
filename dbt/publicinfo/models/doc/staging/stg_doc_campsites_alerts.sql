with source as
(
    select * from {{ source('doc', 'doc_campsites_alerts') }}
),

flattened_alerts as 
(
    select 
        cast(json_data ->> 'assetId' as int) as asset_id,
        json_data ->> 'name' as name,
        cast(json_data ->> 'alerts' as jsonb) as alerts,
        current_timestamp as loaded_at
    from 
        source,
        lateral jsonb_array_elements(raw_data) as json_data
)

select 
    asset_id,
    name,
    cast(alert ->> 'displayDate' as date) as display_date,
    alert ->> 'heading' as heading,
    alert ->> 'detail' as detail,
    loaded_at
from 
    flattened_alerts,
    lateral jsonb_array_elements(alerts) as alert
