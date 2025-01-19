{{
    config(
        materialized = 'table'
    )
}}

with trial_event_data as (
    select space_id
        , user_id
        , event_name
        , event_action
        , event_timestamp
        , event_date
        , days_since_trial_start
        , user_role
        , board_type
    from {{ source('productboard', 'sampling')}}
)

, sorted_events as (
    select *
        , lag(event_timestamp) over (partition by user_id order by event_timestamp) as prev_event_timestamp
    from trial_event_data
)

, session_flags as (
    select *
    , timestamp_diff(event_timestamp, prev_event_timestamp, SECOND) as time_diff_seconds
    -- A session starts if there is no previous event or the time gap exceeds 30 mins
    , CASE
        WHEN prev_event_timestamp IS NULL or timestamp_diff(event_timestamp, prev_event_timestamp, SECOND) >= 1800 then 1
        else 0 
        end  as is_session_start
    from sorted_events
)

, session_ids as (
    select *
    -- Assign session IDs by sequential session count
    , sum(is_session_start) over (partition by user_id order by event_timestamp) as session_id
    from session_flags
)

select * from session_ids


