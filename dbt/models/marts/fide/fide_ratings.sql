-- fide_ratings.sql
{{ 
    config(
        materialized="incremental",
        unique_key=["fide_id", "game_format", "rating_period_ymd"],
        partition_by={
            "field": "rating_period_ymd",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by="fide_id"
    ) 
}}

with
    fide_ratings as (
        select
            -- ids
            fide_id,
            -- strings
            game_format,
            -- numerics
            rating,
            game_count as game_count_during_period,
            k,
            -- timestamps/dates
            rating_period_ymd
        from {{ ref('stg_fide__ratings') }}
        order by rating_period_ymd desc, rating desc, fide_id
    )

select * from fide_ratings
