CREATE TABLE seller (
    id SERIAL PRIMARY KEY,
    seller_id INT,
    seller_name VARCHAR(255),
    seller_location VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    version INT DEFAULT 1
);

-- Type 2 SCD

UPDATE seller
SET valid_to = NOW(), is_active = FALSE
WHERE seller_id = 1809 AND version = (
select max(version) from seller where seller_id = 1809
);

INSERT INTO seller (seller_id, seller_name, seller_location, valid_from, version)
SELECT seller_id, 'Amit Singh', 'Chennai, TN 600100', NOW(), version + 1
FROM seller
WHERE version = (
  SELECT MAX(version)
  FROM seller
  WHERE seller_id = 1809
) and seller_id = 1809;

-- There is an issue, seller_id can not be used as table's primary key

-- Type 1 SCD

MERGE INTO seller AS target
USING (
  VALUES
    (3642, 'Ram mohan', '72560 Robert Views,Teresaborough, PW 57917')
) AS source (seller_id, seller_name, seller_location)
ON target.seller_id = source.seller_id AND target.is_active = true

-- If matched and changed → expire old and insert new
WHEN MATCHED AND (
  target.seller_name IS DISTINCT FROM source.seller_name OR
  target.seller_location IS DISTINCT FROM source.seller_location
) THEN
  UPDATE SET
    source.seller_name,
    source.seller_location

-- Insert the new version
WHEN NOT MATCHED THEN
  INSERT (seller_id, seller_name, seller_location, valid_from, valid_to, is_active, version)
  VALUES (
    source.seller_id,
    source.seller_name,
    source.seller_location,
    NOW(),
    '9999-12-31 23:59:59',
    TRUE,
    1
  );