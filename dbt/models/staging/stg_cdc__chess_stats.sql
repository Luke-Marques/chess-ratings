-- stg_cdc__chess_stats.sql
{{
    config(
        materialized='view',
    )
}}

with
    source as (
        select * from {{ source('chess_ratings', 'landing_cdc__chess_stats') }}
    ),
    all_chess_stats as (
        select
            -- ids
            row_number() over (partition by player_id, game_type, time_control, last_rating, last_date) as instance_number,
            player_id as cdc_id,
            -- strings
            game_type,
            time_control,
            best_game as highest_rated_game_url,
            -- numerics
            cast(last_rating as int) as rating,
            cast(last_rd as int) as rd,
            cast(best_rating as int) as highest_rating,
            cast(record_win as int) as win_count_total,
            cast(record_loss as int) as loss_count_total,
            cast(record_draw as int) as draw_count_total,
            record_time_per_move as avg_time_per_move_in_seconds,
            record_timeout_percent as avg_timeout_percent_last_90_days,
            -- timestamps
            last_date as rating_at,
            best_date as highest_rating_at,
            scrape_datetime as fetched_at,
        from source
        order by fetched_at desc
    ),
    latest_chess_stats as (
        select
            cdc_id,
            game_type,
            time_control,
            highest_rated_game_url,
            rating,
            rd,
            highest_rating,
            win_count_total,
            loss_count_total,
            draw_count_total,
            avg_time_per_move_in_seconds,
            avg_timeout_percent_last_90_days,
            rating_at,
            highest_rating_at,
            fetched_at,
        from all_chess_stats
        where
            (instance_number = 1)
            and ((rating is not null) or (highest_rating is not null))
        order by cdc_id, game_type, time_control, fetched_at desc
    )

select * from latest_chess_stats