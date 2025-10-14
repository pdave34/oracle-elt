drop table "PDAVE"."RAW_FHV_TRIPDATA" cascade constraints purge;
drop table "PDAVE"."RAW_FHVHV_TRIPDATA" cascade constraints purge;
drop table "PDAVE"."RAW_GREEN_TRIPDATA" cascade constraints purge;
drop table "PDAVE"."RAW_YELLOW_TRIPDATA" cascade constraints purge;
drop table "PDAVE"."STG_TEST" cascade constraints purge;

commit;