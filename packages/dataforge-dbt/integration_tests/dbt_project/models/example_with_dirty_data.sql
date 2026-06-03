{{ config(post_hook="{{ dataforge.dataforge_repair('column_x', mode='dry_run') }}") }}

select
    id,
    column_x
from {{ ref('dirty_decimal_shift') }}
