drop table reviews;
drop table TopTenProductsPerYear;
drop table WordCountPerProduct;

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

create table TopTenProductsPerYear as
    select t1.ProductId, t1.conta, t1.anno, t1.prodRank
    from (
        select rew.ProductId, count(rew.ProductId) as conta, year(from_unixtime((rew.ReviewTime))) as anno,
               row_number() over (partition by year(from_unixtime((rew.ReviewTime))) order by count(rew.ProductId) desc) as prodRank
        from reviews rew
        where rew.Id is not null
        group by year(from_unixtime((rew.ReviewTime))), rew.ProductId
    ) t1
    where t1.prodRank <= 10
    order by anno, prodRank;

create table WordCountPerProduct as
    select r.ProductId, r.Text, year(from_unixtime((r.ReviewTime))) as anno
    from reviews r
    where r.ProductId in  (
        select t.ProductId
        from TopTenProductsPerYear t
        where t.anno = year(from_unixtime((r.ReviewTime)))
    )