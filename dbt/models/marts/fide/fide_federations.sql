-- fide_ratings.sql
{{ 
    config(
        materialized="table"
    ) 
}}

with
    fide_ratings_per_fed as (
        select
            -- strings
            fide_federation,
            game_format,
            -- numerics
            count(distinct(fide_id)) as player_count,
            avg(rating) as rating_avg,
            min(rating) as rating_min,
            max(rating) as rating_max,
            -- timestamps/dates
            rating_period_ymd
        from {{ ref('stg_fide__ratings') }}
        group by fide_federation, game_format, rating_period_ymd
        order by fide_federation
    )

select * from fide_ratings_per_fed
