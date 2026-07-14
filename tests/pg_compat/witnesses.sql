SELECT json_object('a' VALUE 1);
MERGE INTO target t USING source s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET value = s.value
WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value);
CREATE TABLE pg18_generated_virtual (
  id int PRIMARY KEY,
  value int GENERATED ALWAYS AS (id + 1) VIRTUAL
);
CREATE TABLE pg18_not_enforced (
  id int,
  CONSTRAINT pg18_not_enforced_check CHECK (id > 0) NOT ENFORCED
);
CREATE TABLE pg18_not_null_no_inherit (
  id int,
  NOT NULL id NO INHERIT
);
CREATE TABLE pg18_temporal_parent (
  id int,
  valid_at daterange,
  PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
CREATE TABLE pg18_temporal_unique (
  id int,
  valid_at daterange,
  UNIQUE (id, valid_at WITHOUT OVERLAPS)
);
CREATE TABLE pg18_temporal_child (
  id int,
  valid_at daterange,
  parent_id int,
  FOREIGN KEY (parent_id, PERIOD valid_at)
    REFERENCES pg18_temporal_parent (id, PERIOD valid_at)
);
INSERT INTO foo VALUES (4, 'conflict')
  ON CONFLICT (f1) DO UPDATE SET f2 = excluded.f2
  RETURNING WITH (OLD AS old_row, NEW AS new_row) old_row.*, new_row.*;
SELECT 1 IN (1, 2), 3 NOT IN (SELECT 4);
ALTER TABLE pg18_not_enforced ALTER CONSTRAINT pg18_not_enforced_check INHERIT;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON LARGE OBJECTS TO public;
CREATE OPERATOR CLASS pg18_int4_ops
DEFAULT FOR TYPE int4 USING btree AS
  OPERATOR 1 < (int4, int4) FOR SEARCH,
  FUNCTION 1 btint4cmp(int4, int4);
VACUUM pg18_generated_virtual (id);
