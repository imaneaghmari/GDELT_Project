CREATE KEYSPACE IF NOT EXISTS gdelt 
WITH REPLICATION = {
             'class': 'NetworkTopologyStrategy',
             'us-east': 2 };

CREATE TABLE IF NOT EXISTS gdelt.event_by_day (
    date text,
    country text,
    language text,
    count_events int,
    PRIMARY KEY (date, country, language)
);
