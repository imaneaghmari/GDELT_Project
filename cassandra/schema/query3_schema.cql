CREATE KEYSPACE IF NOT EXISTS gdelt 
WITH REPLICATION = {
             'class': 'NetworkTopologyStrategy',
             'us-east': 2 };

CREATE TABLE IF NOT EXISTS gdelt.article_by_theme (
    source_common_name text,
    year int,
    month int,
    day int,
    theme text,
    num_article int,
    sum_tone int,
    PRIMARY KEY (source_common_name, year, month, day, theme)
);

CREATE TABLE IF NOT EXISTS gdelt.article_by_person (
    source_common_name text,
    year int,
    month int,
    day int,
    person text,
    num_article int,
    sum_tone int,
    PRIMARY KEY (source_common_name, year, month, day, person)
);

CREATE TABLE IF NOT EXISTS gdelt.article_by_location (
    source_common_name text,
    year int,
    month int,
    day int,
    location text,
    num_article int,
    sum_tone int,
    PRIMARY KEY (source_common_name, year, month, day, location)
);