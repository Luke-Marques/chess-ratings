-- stg_cdc__profiles.sql
{{
    config(
        materialized='view',
    )
}}

with
    source as (
        select * from {{ source('chess_ratings', 'landing_cdc__profiles') }}
    ),
    all_profile_records as (
        select
            -- ids
            row_number() over (partition by player_id) as instance_number,
            player_id as cdc_id,
            -- strings
            username,
            name,
            country,
            location,
            profile_url,
            avatar_url,
            api_url,
            title,
            -- numerics
            followers,
            -- booleans
            is_streamer,
            -- timestamps
            joined as joined_at,
            last_online as last_online_at,
            scrape_datetime as fetched_at
        from source
        order by fetched_at desc
    ),
    latest_profile_records as (
        select
            cdc_id,
            username,
            name,
            country,
            location,
            profile_url,
            avatar_url,
            api_url,
            title,
            followers,
            is_streamer,
            joined_at,
            last_online_at,
            fetched_at
        from all_profile_records
        where instance_number = 1
        order by fetched_at desc
    )

select * from latest_profile_records