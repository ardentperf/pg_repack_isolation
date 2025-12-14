\set id random(1, 10000000)
UPDATE user1_schema.source_data SET data = 'Updated-' || lpad(:id::text, 8, '0') WHERE id = :id;
