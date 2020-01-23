CREATE KEYSPACE IF NOT EXISTS gdelt 
WITH REPLICATION = {
             'class': 'NetworkTopologyStrategy',
             'us-east': 2 };

CREATE TABLE IF NOT EXISTS gdelt.country_map (
    country_code text,
    translation_info text,
    year int,
    month int,
    date int,
    num_article int,
    sum_tone float,
    PRIMARY KEY ((translation_info, country_code), year, month, day)
);

