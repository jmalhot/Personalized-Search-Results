# Personalized-Search-Results
Personalized Search Results using Spark, Mongo, LSH

- 1+ Million record are loaded into Spark dataframe from Mongodb
- Popular items are loaded from user behavioural logs stored in S#
- Popular items are used to find most similar items from Mongodb database.
- Similarity is computed using Local Sensitive Hashing concept
- Code is develoepd using SPark framework
- Code can run on AWS EMR or GCP Dataprocs.
