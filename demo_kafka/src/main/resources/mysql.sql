drop table if EXISTS  test.gp_app_log;

create table test.gp_app_log(

        batchId int AUTO_INCREMENT primary key,
        app_name varchar(64) DEFAULT  'gupao',
        time varchar(128),
        level varchar(128),
        thread_name varchar(128),
        method_name varchar(128),
        error_msg varchar(128)
)