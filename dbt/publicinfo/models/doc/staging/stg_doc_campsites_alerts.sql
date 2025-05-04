with source as
(
    select 
        raw_data, 
        row_number() over (partition by file_name order by id desc) as row_number 
    from {{ source('doc', 'doc_campsites_alerts') }}
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
    where 
        source.row_number = 1
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
