create table comment_table
(
    "id"                  int         not null,
    "first_name"          varchar(50) not null,
    "last_name"           varchar(50),
    "address"             varchar(50) default null,
    "created"             timestamp   not null,
    "updated"             timestamp,
    "decimal_field"       decimal(20, 3),
    "other_decimal_field" decimal
);

comment on table comment_table is 'Table with comments.';
comment on column comment_table.id is 'Id for the comment table.';