
DROP TABLE IF EXISTS reviews;
DROP TABLE IF EXISTS allUsers;
DROP TABLE IF EXISTS groups;
DROP TABLE IF EXISTS result;

CREATE TABLE IF NOT EXISTS reviews (
    Id INT,
    ProductId STRING,
    UserId STRING,
    ProfileName STRING,
    HelpfulnessNumerator INT,
    HelpfulnessDenominator INT,
    Score INT,
    Time INT,
    Summary STRING,
    Text STRING
)
COMMENT 'Cleaned Reviews Table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/Users/eros/Downloads/BigDataProject1/CleanedDataset.csv' OVERWRITE INTO TABLE reviews;


CREATE TABLE allUsers AS
SELECT UserId, Collect_Set(ProductId) AS products
FROM reviews
WHERE Score >= 4
GROUP BY UserId
HAVING COUNT(*) >= 3;

CREATE TABLE groups AS
SELECT Collect_Set(UserId) AS users , Collect_Set(ProductId) AS common_products
FROM all_Users u
JOIN reviews r
ON u.UserId != r.UserId AND ARRAY_CONTAINS(u.products, r.ProductId)
GROUP BY u.UserId;

CREATE TABLE result AS
SELECT g.users, g.common_products
FROM groups
HAVING COUNT(users)>1;


SELECT *
FROM result;


DROP TABLE reviews;
DROP TABLE allUsers;
DROP TABLE groups;
-- DROP TABLE result;