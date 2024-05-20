-- fide_profiles.sql
{{ 
    config(
        materialized="table",
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
    chess_records as (
        select 
            -- ids
            cdc_id,
            -- strings
            game_type,
            time_control,
            highest_rated_game_url,
            -- numerics
            highest_rating,
            win_count,
            loss_count,
            draw_count,
            avg_time_per_move_in_seconds,
            avg_timeout_percent_last_90_days,
            -- timestamps
            highest_rated_game_at,
            fetched_at,
        from cdc_chess_stats
        where instance_number = 1
        order by cdc_id
    )

select * from chess_records
