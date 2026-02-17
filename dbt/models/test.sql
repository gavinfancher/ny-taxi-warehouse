{{ config(materialized='table') }}

select *
from {{ source('raw', 'yellow_trips') }}
limit 2
