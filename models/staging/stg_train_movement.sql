select
    *
from {{ source('fchg_data', 'train_movement') }}
