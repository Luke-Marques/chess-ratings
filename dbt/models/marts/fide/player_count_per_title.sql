-- fide_profiles.sql
{{ 
    config(
        materialized="table",
    ) 
}}

with
    fide_ratings as  (
        select
            *,
            row_number() over (partition by fide_id order by rating_period_ymd desc) as instance_number,
        from {{ ref('stg_fide__ratings') }}
    ),
    fide_profiles as (
        select 
            ifnull(title, "Untitled") as title,
            game_format,
            count(distinct(fide_id)) as player_count,
        from fide_ratings
        where instance_number = 1
        group by title, game_format
        order by title, game_format
    )

select * from fide_profiles
