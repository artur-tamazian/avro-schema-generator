create table array_table
(
    id          int        not null,
    addresses11 varchar[],
    addresses12 varchar[]  not null,
    addresses21 varchar[3],
    addresses22 varchar[3] not null,
    ages11 int ARRAY,
    ages12 int ARRAY[3]
);
