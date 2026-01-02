create role if not exists access;
grant role access to role accountadmin;
create warehouse if not exists cal_wh with warehouse_size="x-small";
grant operate on warehouse cal_wh to role access;
create user if not exists dbt
    password='music123'
    login_name='music_rec'
    must_change_password=false
    default_warehouse='cal_wh'
    default_role=access
    default_namespace='musicdata_raw'
    comment='controlled usage to transform data';
alter user dbt set type=legacy_service;
grant role access to user dbt;
create database if not exists musicdata;
create schema if not exists musicdata.raw;
create schema if not exists musicdata.analytics;
grant all on warehouse cal_wh to role access;
grant all on database musicdata to role access;
grant all on all schemas in database musicdata to role access;
grant all on future schemas in database musicdata to role access;
grant all on all tables in schema musicdata.raw to role access;
grant all on future tables in schema musicdata.raw to role access;