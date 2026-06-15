SELECT json_object('a' VALUE 1);
MERGE INTO target t USING source s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET value = s.value
WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value);
