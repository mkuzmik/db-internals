--- PAGE ---

-- Define table
DROP TABLE IF EXISTS person;
CREATE TABLE person
(
    id   serial primary key,
    name varchar(255) NOT NULL
);


-- Fill it with data
INSERT INTO person(name)
VALUES ('Michael Scott'),
       ('Dwight Schrute'),
       ('Stanley Hudson');


-- Lookup the data
SELECT *
FROM person;


-- Lookup the data with tuple id
SELECT ctid, *
FROM person;
-- ctid (page_number, offset)


-- Update the first row
UPDATE person
SET name = 'Michael Klump'
WHERE name = 'Michael Scott';


-- Lookup the data with tuple id
SELECT ctid, *
FROM person;

-- Case 1 - why tuple id hs changed? Why order has changed?


-- Load pageinspect extension, which allows us to see details of the pages
CREATE EXTENSION IF NOT EXISTS pageinspect;


-- Display the content of page 0
SELECT page
FROM get_raw_page('person', 0) as page;
-- copy it into text file
-- Bonus material, how data is aligned into a page: https://www.interdb.jp/pg/pgsql01.html#_1.3.


-- A proof that single page has 8kB, no matter how many rows it stores
SELECT pg_column_size(page)
FROM get_raw_page('person', 0) as page;


-- More readable way to inspect the page - heap_page_items
SELECT *
FROM heap_page_items(get_raw_page('person', 0));


--- MVCC ---

SELECT lp, t_ctid, t_xmin, t_xmax, t_data
FROM heap_page_items(get_raw_page('person', 0));


-- current transaction id (txid)
SELECT txid_current();


-- remove tuples that are no longer needed
VACUUM FULL;


-- reading tuple flags
SELECT heap_tuple_infomask_flags(t_infomask, t_infomask2)
FROM heap_page_items(get_raw_page('person', 0));
-- all tuples are frozen (t_xmin not comparable with newer txids)


--- INDEX ---

-- There is a default index on primary key (id)
select *
from bt_page_items('person_pkey', 1);

-- utils
CREATE OR REPLACE FUNCTION array_reverse(anyarray) RETURNS anyarray AS
$$
SELECT ARRAY(
               SELECT $1[i]
               FROM generate_subscripts($1, 1) AS s(i)
               ORDER BY i DESC
       );
$$ LANGUAGE 'sql' STRICT
                  IMMUTABLE;

CREATE OR REPLACE FUNCTION hex_to_int(hexval varchar) RETURNS integer AS
$$
DECLARE
    result int;
BEGIN
    EXECUTE 'SELECT x' || quote_literal(hexval) || '::int' INTO result;
    RETURN result;
END;
$$ LANGUAGE plpgsql IMMUTABLE
                    STRICT;

CREATE OR REPLACE FUNCTION data_to_int(text) RETURNS int AS
$$
SELECT hex_to_int(array_to_string(array_reverse(string_to_array(left($1, 11), ' ')), ''));
$$ LANGUAGE 'sql' STRICT
                  IMMUTABLE;

-- More readable way
select data_to_int(data) as key, ctid
from bt_page_items('person_pkey', 1);


-- generate a lot of data for the index
INSERT INTO person(name)
SELECT md5(random()::text)
FROM generate_series(1, 1000000);

-- confirm that data is there
SELECT count(*)
FROM person;

-- Task - find id = 500000 in the btree

-- Read root page number from the metapage
SELECT root
FROM bt_metap('person_pkey');


select data_to_int(data) as key,
       ctid,
       dead
from bt_page_items('person_pkey', 412);


select data_to_int(data) as key,
       ctid,
       dead
from bt_page_items('person_pkey', 984);


select data_to_int(data) as key,
       ctid,
       dead
from bt_page_items('person_pkey', 1098);

SELECT *
FROM person
WHERE ctid = '(3333,40)';

--- HOT ---

-- noinspection SqlWithoutWhere
UPDATE person
SET name = 'Kelly Kapoor';
-- benchmark

DROP TABLE IF EXISTS person_v2;
CREATE TABLE person_v2
(
    id   serial primary key,
    name varchar(255) NOT NULL
) WITH (fillfactor=40);

INSERT INTO person_v2(name)
SELECT md5(random()::text)
FROM generate_series(1, 1000000);

-- noinspection SqlWithoutWhere
UPDATE person_v2
SET name = 'Kelly Kapoooor';
-- benchmark with person (v1)


select data_to_int(data) as key,
       ctid,
       dead
from bt_page_items('person_v2_pkey', 1098);

SELECT *
FROM heap_page_items(get_raw_page('person_v2', 8503));