# Personalized-Search-Results
Personalized Search Results using Spark, Mongo, LSH, AWS S3

- 1+ Billion record are loaded into Spark dataframe from Mongodb
- Popular items are loaded from user behavioural logs stored in S3
- Popular items are used to find most similar items from Mongodb database.
- Similarity is computed using Local Sensitive Hashing concept
- Code is develoepd using Spark ML framework
- Code can run on AWS EMR Cluster or Google Cloud Platfor's Dataprocs.
