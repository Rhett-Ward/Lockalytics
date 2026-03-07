# Lockalytics
School project Big Data Deadlock api and meta analytics


This is a big data attempt to process live match data to keep an update analytics website(UI) thats interactable and filterable and provides up to date analysis. It will use HDFS, MongoDB, Apache Spark, and both the steam api and the community made deadlock api.


2/16/2026-
First successful api call from Deadlock community API
Exported data call to a csv file via python and created a google sheets script to begin converting some data to be readable and consumable.
worked with api dev to spot some bugs.
emailed prof for rescoping advice.

2/24/2026-
got a test db running using atlas
modified my existing python api test script to access said db cluster and add data as a collection
successfully imported csv of hero stats into cluster from python script

3/3/2026
deployed a hadoop hdfs docker container and began learning how to interact with it.
next steps:
setup apache spark to stream from api (first iteration might just be making calls on a timer or making a call with pyspark at all)
Attached HDFS container to AZUR blob storage
create mongodb atlas instance through azure that is linked to that blob storage
rewrite project statement

3/6/2026
made a batch script that connects everything together, has requirements in it, can auto install just about everything (just need docker, npm, and python before hand) and does all the work
the end result is a hdfs and mongo db cluster that host a csv and a website that pulls from it and is created inside of a node js script. have to figure out how to upload it all to github due to data limits. 
have to write report still
have to figure out what can and cant be uploaded (like atlas db key)
