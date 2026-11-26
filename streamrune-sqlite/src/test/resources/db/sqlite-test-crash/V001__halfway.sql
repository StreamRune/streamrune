-- A test series: two tables, then an insert the test can make fail, so the migration fails after
-- the script has already done work.
CREATE TABLE first_half (id INTEGER PRIMARY KEY) STRICT;
CREATE TABLE second_half (id INTEGER PRIMARY KEY) STRICT;
INSERT INTO test_gate (open) VALUES (1);
