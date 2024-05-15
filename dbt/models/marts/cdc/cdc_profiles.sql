-- fide_profiles.sql
{{ 
    config(
        materialized="table",
        partition_by={
            "field": "title",
            "data_type": "string",
        },
    ) 
}}

with
    all_cdc_profiles as (
        select 
            *,
            row_number() over (partition by cdc_id) as instance_number
        from {{ ref('stg_cdc__profiles' )}}
        order by fetched_at desc
    ),
    latest_cdc_profiles as (
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
            status,
            twitch_url,
            followers,
            is_streamer,
            joined_at,
            last_online_at,
            fetched_at
        from all_cdc_profiles
        where instance_number = 1
        order by fetched_at desc
    )

select * from latest_cdc_profiles
