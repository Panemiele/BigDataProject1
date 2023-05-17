drop table reviews;
drop table TopTenProductsPerYear;
drop table ProductReviewPerYear;
drop table Result;

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

create table ProductReviewPerYear as
    select r.ProductId, regexp_replace(r.Text, "\\t", "") as text , TTPPY.anno
    from reviews r join TopTenProductsPerYear TTPPY on r.ProductId = TTPPY.ProductId;

create table Result as
    select d.anno, d.ProductId, d.word, d.conta
    from (
        select t.anno, t.ProductId, word, count(word) as conta, row_number() over (partition by t.anno, t.ProductId order by count(word) desc) as prodRank
        from ProductReviewPerYear t LATERAL VIEW explode(split(lower(text), ' ')) singleword AS word
        where length(word) >= 4
        group by t.ProductId, t.anno, word
    ) d
    where prodRank <= 5;