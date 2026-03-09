# Lockalytics
School project Big Data Deadlock api and meta analytics


This is a big data attempt to process live amatch data to keep an update analytics website(UI) thats interactable and filterable and provides up to date analysis. It will use HDFS, MongoDB, Apache Spark, and both the steam api and the community made deadlock api.

# "Table of contents"
- [Requirements](#Requirements)
- Setup / Instructions
- [Progress Reports](#Progress-Reports)


# Requirements

- [Docker Desktop](https://docs.docker.com/desktop/) Needed for deploying the HDFS server
  - [Windows Installer](https://docs.docker.com/desktop/setup/install/windows-install/)
  - [Mac Installer](https://docs.docker.com/desktop/setup/install/mac-install/)
  - [Linux Installer](https://docs.docker.com/desktop/setup/install/linux/)
- [Mongo DB Instance](https://www.mongodb.com/products/platform/atlas-database) You'll need to set one up if you want to run it yourself. [Step by step guide on setting it up](###Step-4)
- [Node.js / NPM] It is very likely that you already have NPM installed if your on windows but just in case you dont
 - [Instructions on how to install NPM](https://docs.npmjs.com/downloading-and-installing-node-js-and-npm) 
- [Python3] Its very likely you already have python installed but in case you dont
 - [Python for Windows](https://www.python.org/downloads/windows/) Click on the Installer (MSIX) of the most recent stable version
 - [Python for MAC](https://www.python.org/downloads/macos/) Click on the Installer of the most recent stable version
 - [Python for Linux](https://www.python.org/downloads/source/) Might be easier to just do `sudo apt install python3` 
- [Hadoop Zip File]()
- [Batch File]() If you are willing to run this on your computer you can see everything in action (code is mostly readable even if unknowledgeable, its completely safe but feel free to open it in notepad to check it out)

# Instuctions
## Here is a step by step guide of how to get it all going

### Step 1
Download and install all the requirements

### Step 2
Open Docker Desktop and sign in / create an account if you haven't already

### Step 3
Make sure you can see this in the bottom left of your docker desktop window

<img src="https://media.discordapp.net/attachments/723371382091546645/1480376147123638424/image.png?ex=69af7339&is=69ae21b9&hm=2a13eff637cd81e5eb9e9825346dd9da79bdf140b5a5bcc54bfe1b63f6b4ecd1&=&format=webp&quality=lossless&width=138&height=30" alt="Engine Running Symbol" width=300 height=300>

### Step 4 
create a MongoDB Cluster



# Progress Reports

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
