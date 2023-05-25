ADD JAR /mnt/c/ProgettiIntellij/brickhouse/target/brickhouse-0.7.1-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION array_union AS 'brickhouse.udf.collect.ArrayUnionUDF';

DROP TABLE IF EXISTS userToProds;
DROP TABLE IF EXISTS usersCommonProds;
DROP TABLE IF EXISTS groupUsersCommonProds;
DROP TABLE IF EXISTS result;

CREATE TABLE IF NOT EXISTS reviews (
Id INT,
ProductId STRING,
UserId STRING,
HelpfulnessNumerator INT,
HelpfulnessDenominator INT,
Score INT,
ReviewTime INT,
Text STRING
)
COMMENT 'Cleaned Reviews Table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/mnt/c/Users/Gabri/OneDrive/Documenti/Università/BigData/Progetti/Progetto1/Dataset/CleanedDataset.csv' OVERWRITE INTO TABLE reviews;

CREATE TABLE userToProds as
    SELECT UserId, ProductId
    FROM reviews
    WHERE Score >= 4;

CREATE TABLE usersCommonProds as
    select a.UserId as user1, b.UserId as user2, a.ProductId as commonProd
    FROM userToProds a join userToProds b on (a.ProductId = b.ProductId and a.UserId != b.UserId);

CREATE TABLE groupUsersCommonProds as
    select user1, user2, collect_set(commonProd) as commonProducts
    from usersCommonProds
    group by user1, user2
    having size(collect_set(commonProd)) >= 3;

CREATE TABLE partialResult as
    select user1, collect_set(user2) as otherUsers, commonProducts
    from groupUsersCommonProds
    group by user1, commonProducts
    order by user1 desc;

CREATE TABLE result as
    select array_union(array(user1), otherUsers), commonProducts
    from partialResult;