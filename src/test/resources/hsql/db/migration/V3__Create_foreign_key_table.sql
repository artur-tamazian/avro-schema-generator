create table "test_foreign_key_root"(
    "id" int not null primary key,
    "name" varchar(50),
);

create table "test_foreign_key_child"(
    "id" int not null primary key,
    "name" varchar(50),
    "root_id" int not null,
    foreign key ("root_id") references "test_foreign_key_root"("id")
);
