#!/usr/bin/env bash

create_namespace 'realtime'

create 'realtime:metric_user_ratio','info'
create 'realtime:metric_total_price','info'
create 'realtime:metric_register_user_cnt','info'
create 'realtime:metric_price_by_prd','info'

