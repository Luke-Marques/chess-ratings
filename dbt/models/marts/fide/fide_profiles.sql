-- fide_profiles.sql
{{ 
    config(
        materialized="incremental",
        unique_key="fide_id",
        partition_by={
            "field": "title",
            "data_type": "string"
        }
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
            -- ids
            fide_id,
            -- strings
            name,
            fide_federation,
            sex,
            title,
            w_title,
            o_title,
            foa_title,
            flag,
            -- numerics
            sum(game_count) over (partition by fide_id) as total_game_count,
            birth_year
        from fide_ratings
        where instance_number = 1
        order by fide_id, rating_period_ymd desc
    )

select * from fide_profiles
