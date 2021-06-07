with source as (
    SELECT *
        FROM {{ ref('state') }}
),

stage_state as (
    SELECT
        st,
        state_name
    FROM source
)

SELECT *
    FROM stage_state