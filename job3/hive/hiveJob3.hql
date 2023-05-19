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
    SELECT a1.UserId AS user1, a2.UserId AS user2
    FROM allUsers a1
    JOIN allUsers a2
    ON a1.UserId < a2.UserId;

CREATE TABLE result AS
    SELECT r.UserId, r.ProductId
    FROM reviews r
    JOIN groups g
    ON r.UserId = g.user1 OR r.UserId = g.user2;