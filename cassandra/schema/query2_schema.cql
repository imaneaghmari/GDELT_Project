CREATE KEYSPACE IF NOT EXISTS gdelt 
WITH REPLICATION = {
             'class': 'NetworkTopologyStrategy',
             'us-east': 2 };
             
CREATE TABLE IF NOT EXISTS gdelt.country_events (
  country text,
  year int,
  month int,
  day int,
  event int,
  num_mentions int,
  PRIMARY KEY (country, year, month, day)
);
