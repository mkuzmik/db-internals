------ UTILS ---------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS pageinspect;

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

--------------------------------------------------------------------------------------------


-- START
----------------------------------------------------------

DROP TABLE IF EXISTS person;
CREATE TABLE person
(
    id   serial primary key,
    name varchar(255) NOT NULL
);

INSERT INTO person(name)
VALUES ('Kapitan Bomba');
INSERT INTO person(name)
VALUES ('Chorąży Torpeda');
INSERT INTO person(name)
VALUES ('Sułtan Kosmitów');

SELECT ctid, * FROM person;
-- ctid (page_no, offset)

-- ctid - tuple id (NR_STRONY, OFFSET)

----------------------------------------------------------

UPDATE person
SET name = 'Matka Sultana Kosmitów'
WHERE name = 'Sułtan Kosmitów';

SELECT ctid, *
FROM person WHERE ctid = (0,4);

SELECT page FROM get_raw_page('person', 0) as page;
-- whyyyy: https://www.interdb.jp/pg/pgsql01.html#_1.3.

SELECT pg_column_size(page) FROM get_raw_page('person', 0) as page;

-----------------------------------------------------------

SELECT '(0,' || lp || ')' as ctid,
       t_ctid             as reference_to_ctid,
       t_xmin,
       t_xmax,
       t_data
FROM heap_page_items(get_raw_page('person', 0));

SELECT * FROM heap_page_items(get_raw_page('person', 0));

-- mvcc - multi-version concurrency control

select data_to_int(data) as key, ctid
from bt_page_items('person_pkey', 1);

-- Pytanie: Dlaczego klucz 3 w indeksie nadal wskazuje na ctid (0,3)?

-- Odpowiedź: Bo HOT: https://www.interdb.jp/pg/pgsql07.html#_7.1.

-------------------------------------------------------------

DELETE
FROM person
WHERE name = 'Chorąży Torpeda';

SELECT '(0,' || lp || ')' as ctid,
       t_ctid             as reference_to_ctid,
       t_xmin,
       t_xmax,
       t_data
FROM heap_page_items(get_raw_page('person', 0));

select data_to_int(data) as key, ctid
from bt_page_items('person_pkey', 1);

--------------------------------------------------------------

VACUUM FULL;

SELECT '(0,' || lp || ')' as ctid,
       t_ctid             as reference_to_ctid,
       t_xmin,
       t_xmax,
       t_data
FROM heap_page_items(get_raw_page('person', 0));

select data_to_int(data) as key, ctid
from bt_page_items('person_pkey', 1);

---------------------------------------------------------------

INSERT INTO person(name)
SELECT md5(random()::text)
FROM generate_series(1, 1000000);
ANALYSE person;

EXPLAIN
SELECT *
FROM person
WHERE id = 50;

EXPLAIN
SELECT *
FROM person
WHERE id > 100000;

-- pytanie: dlaczego w drugim zapytaniu nie patrzymy na indeks?

---- mastering btree -----------------------------------------------

-- znajdźmy teraz klucz id=400'000 w indeksie btree

SELECT *
FROM bt_metap('person_pkey');

select data_to_int(data) as key,
       ctid,
       dead
from bt_page_items('person_pkey', 412);

select data_to_int(data) as key,
       ctid,
       dead
from bt_page_items('person_pkey', 984);
-- pierwszy wiersz to odnośnik do następnej strony
-- drugi wiersz to odnośnik do poprzedniej strony

select data_to_int(data) as key,
       ctid,
       dead
from bt_page_items('person_pkey', 1098);

SELECT *
FROM person
WHERE ctid = '(3333,39)';

----------------------------------------

UPDATE person
SET name='kapitan bomba'
WHERE id = 400000;

SELECT *
FROM person
WHERE ctid = '(3333,39)';

select data_to_int(data) as key,
       ctid,
       dead
from bt_page_items('person_pkey', 1098)
WHERE data_to_int(data) = 400000;

SELECT *
FROM person
WHERE ctid = '(8333,43)';
