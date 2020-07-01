create table "test_comments"(
    "id" int not null,
    "name" varchar(50),
    "created" timestamp not null,
    "updated" timestamp,
    "decimal_field" decimal(20, 3),
    "other_decimal_field" decimal
);

comment on table "test_comments" is 'Table with comments.';
comment on column "test_comments"."id" is 'Id for the test_comments table.';