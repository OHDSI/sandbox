post_etl_stats_v5.sql and post_etl_review.sql

post_etl_stats_v5 is expected to be run after each ETL.  It captures a number of statistics.

post_etl_review.sql will compare the statistics saved by post_etl_stats to the those of the prior ETL allowing a quick comparison of ETLs.