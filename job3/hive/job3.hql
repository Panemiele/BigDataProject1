drop table reviews;
drop table result;

CREATE TABLE IF NOT EXISTS reviews (
Id int,
ProductId string,
UserId string,
HelpfulnessNumerator int,
HelpfulnessDenominator int,
Score int,
ReviewTime int,
Text string)
COMMENT 'Cleaned Reviews Table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/mnt/c/Users/Gabri/OneDrive/Documenti/Università/BigData/Progetti/Progetto1/Dataset/CleanedDataset.csv' overwrite INTO TABLE reviews;

create table TopProductsPerYear as
    select year(from_unixtime(Time)) as anno, count(ProductId)
    from reviews

create table result as
    select utente, avg(num * 1.0 / denom) as appr
    from userReviewInfo
    group by utente
    order by appr desc;