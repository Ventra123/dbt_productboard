{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {'field': 'event_date', 'data_type': 'date'}
    )
}}

with trial_event_data as (
    select
        ps.space_id,
        ps.user_id,
        ps.event_name,
        ps.event_action,
        ps.event_timestamp,
        ps.event_date,
        ps.days_since_trial_start,
        ps.user_role,
        ps.board_type
    from {{ source('productboard', 'sampling') }} as ps
    {% if is_incremental() %}
        where ps.event_timestamp > (select max(event_timestamp) from {{ this }}) -- noqa
    {% endif %}
),

-- Get prior session info during each incremental run
-- Extract numerical session id for each user
{% if is_incremental() %}
    last_processed_status as (
        select
            user_id,
            max(event_timestamp) as last_event_timestamp,
            max(cast(split(session_id, '-')[offset(1)] as int))
                as last_session_id
        from {{ this }}
        group by user_id
    ),
{% endif %}



current_events as (
    select
        trial_event_data.*,
        {% if is_incremental() %}
            coalesce(last_processed_status.last_session_id, null)
                as last_processed_session_id,
            coalesce(last_processed_status.last_event_timestamp, null)
                as last_processed_event_timestamp
        {% else %}
        null as last_processed_session_id,
        cast(null as timestamp) as last_processed_event_timestamp
        {% endif %}
    from trial_event_data
    {% if is_incremental() %}
        left join
            last_processed_status
            on trial_event_data.user_id = last_processed_status.user_id
    {% endif %}
),

current_events_sorted as (
    select
        *,
        lag(event_timestamp)
            over (
                partition by user_id
                order by event_timestamp
            )
            as current_batch_prev_event_timestamp
    from current_events
),

-- Get last processed timestamp for the very first event in the new batch
session_flags as (
    select
        *,
        timestamp_diff(
            event_timestamp,
            coalesce(
                current_batch_prev_event_timestamp, last_processed_event_timestamp -- noqa: LT05
            ),
            second
        ) as time_diff_seconds,
        -- A session starts if there is no previous event or the time gap exceeds 30 mins -- noqa: LT05
        case
            when
                current_batch_prev_event_timestamp is null
                and last_processed_event_timestamp is null
                then 1
            when
                timestamp_diff(
                    event_timestamp,
                    coalesce(
                        current_batch_prev_event_timestamp,
                        last_processed_event_timestamp
                    ),
                    second
                )
                >= 1800
                then 1
            else 0
        end as is_session_start
    from current_events_sorted
),

session_ids as (
    select
        *,
        -- Assign session IDs by sequential session count
        concat(
            user_id, '-',
            cast(
                coalesce(last_processed_session_id, 0) + sum(is_session_start)
                    over (
                        partition by user_id
                        order by event_timestamp
                    )
                as string
            )
        ) as session_id
    from session_flags
)

select * from session_ids
