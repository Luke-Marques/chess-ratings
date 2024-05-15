-- fide_profiles.sql
{{ 
    config(
        materialized="incremental",
        partition_by={
            "field": "fetched_at",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by="cdc_id",
    ) 
}}

with
    cdc_chess_stats as (
        select 
            *,
            row_number() over (partition by cdc_id) as instance_number
        from {{ ref('stg_cdc__chess_stats') }}
        order by fetched_at desc
    ),
    chess_ratings as (
        select 
            -- ids
            cdc_id,
            -- strings
            game_type,
            time_control,
            -- numerics
            rating,
            rd,
            -- timestamps
            latest_game_at as rating_at,
            fetched_at
        from cdc_chess_stats
        where instance_number = 1
        order by cdc_id, game_type, time_control, rating_at desc
    )

select * from chess_ratings
