create table default_table
(
    "id"                  int         not null,
    "first_name"          varchar(50) not null,
    "last_name"           varchar(50),
    "address"             varchar(50) default null,
    "created"             timestamp   not null,
    "updated"             timestamp,
    "decimal_field"       decimal(20, 3),
    "other_decimal_field" decimal(10,0),
    "cancelled"           timestamp with time zone
);
