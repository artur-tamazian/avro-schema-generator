create table "test_records"(
    "id" int not null,
    "name" varchar(50),
    "created" timestamp not null,
    "updated" timestamp,
    "decimal_field" decimal(20, 3),
    "other_decimal_field" decimal
);