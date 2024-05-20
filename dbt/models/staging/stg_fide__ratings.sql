-- stg_fide__ratings.sql
{{
    config(
        materialized='view',
    )
}}

with
    source as (
        select * from {{ source('chess_ratings', 'landing_fide__ratings') }}
    ),
    fide_ratings as (
        select
            -- ids
            fide_id,
            -- strings
            player_name as name,
            game_format,
            fide_federation,
            sex,
            title,
            w_title,
            o_title,
            foa_title,
            flag,
            -- numerics
            cast(rating as int) as rating,
            cast(game_count as int) as game_count,
            cast(k as int) as k,
            cast(birth_year as int) as birth_year,
            -- timestamps/dates
            date(period_year, period_month, 1) as rating_period_ymd
        from source
        order by rating_period_ymd desc
    )

select * from fide_ratings