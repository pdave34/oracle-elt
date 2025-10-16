
{{
  config(
    materialized='table',
    post_hook=[
      "begin for r in (select * from {{ this }}) loop null; end loop; end;"
    ]
  )
}}

-- Simple test model: select a tiny sample from INIT_REPLEN
select loc_id, item_family, target
from {{ ref('INIT_REPLEN') }}
where rownum <= 1
