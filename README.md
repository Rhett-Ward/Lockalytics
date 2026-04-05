# Lockalytics
School project Big Data Deadlock api and meta analytics


This is a big data attempt to process live amatch data to keep an update analytics website(UI) thats interactable and filterable and provides up to date analysis. It will use HDFS, MongoDB, Apache Spark, and both the steam api and the community made deadlock api.

# "Table of contents"
- [Requirements](#Requirements)
- [Setup / Instructions](#Instructions)
- [Progress Reports](#Progress-Reports)


# Requirements

- [Docker Desktop](https://docs.docker.com/desktop/) Needed for deploying the HDFS server
  - [Windows Installer](https://docs.docker.com/desktop/setup/install/windows-install/)
  - [Mac Installer](https://docs.docker.com/desktop/setup/install/mac-install/)
  - [Linux Installer](https://docs.docker.com/desktop/setup/install/linux/)
- [Mongo DB Instance](https://www.mongodb.com/products/platform/atlas-database) You'll need to set one up if you want to run it yourself. [Step by step guide on setting it up](###Step-4)
- Node.js / NPM It is very likely that you already have NPM installed if your on windows but just in case you dont
 - [Instructions on how to install NPM](https://docs.npmjs.com/downloading-and-installing-node-js-and-npm) 
- Python3 Its very likely you already have python installed but in case you dont
 - [Python for Windows](https://www.python.org/downloads/windows/) Click on the Installer (MSIX) of the most recent stable version
 - [Python for MAC](https://www.python.org/downloads/macos/) Click on the Installer of the most recent stable version
 - [Python for Linux](https://www.python.org/downloads/source/) Might be easier to just do `sudo apt install python3` 
- [Hadoop Zip File](https://github.com/Rhett-Ward/Lockalytics/blob/8e126bdee4f8e16646d5d8667e53285cb9bf8f33/src/Hadoop%20HDFS.zip)
- Batch File - If you are willing to run this on your computer you can see everything in action (code is mostly readable even if unknowledgeable, its completely safe but feel free to open it in notepad to check it out)

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
create a MongoDB Cluster (Project used for screenshots was deleted shortly after, password left uncensored since it no longer correlates to anything, this cluster can't be accessed.)

- Step 1
  - Create a free account and sign in
- Step 2
  - Create a project
  - <img width="900" height="600" alt="image" src="https://github.com/user-attachments/assets/084f54f2-b237-4174-972a-70e3da7ece91" />
  - <img width="480" height="480" alt="image" src="https://github.com/user-attachments/assets/2abeffe2-3392-43ef-9075-a782ea4899df" />
  - <img width="480" height="480" alt="image" src="https://github.com/user-attachments/assets/8f312098-e13a-4c30-86bf-e369c4f36ea0" />
- Step 3
  - Create a Cluster
  - <img width="900" height="600" alt="image" src="https://github.com/user-attachments/assets/057e73e3-0f7b-4665-b27c-1a22d84cd379" />
  - <img width="900" height="600" alt="image" src="https://github.com/user-attachments/assets/b9127986-9f78-4b1e-b580-2ade6a08c9ce" />
    - Provider doesn't matter here
- Step 4
  - Connect to your cluster
    - <img width="480" height="480" alt="image" src="https://github.com/user-attachments/assets/c083a62e-bb83-44e5-b057-6b10aecb8869" />
    - <img width="480" height="480" alt="image" src="https://github.com/user-attachments/assets/7d893298-e246-468b-bd15-e6a91f306910" />
- Step 5
  - Copy your connection strings into the appropriate files.
    - Python
      - <img width="900" height="600" alt="image" src="https://github.com/user-attachments/assets/73ff6291-4459-48f6-8e6f-b91ba862addf" />
      - Copy your version of this string into the below segment of "Testing File.py"
      - <img width="900" height="300" alt="image" src="https://github.com/user-attachments/assets/fc720439-5035-4045-aa5b-1d932a5d626c" />
    - Node.js
      - <img width="900" height="600" alt="image" src="https://github.com/user-attachments/assets/17b81e14-61da-4c7c-875c-5a8a54354b88" />
      - Copy your version of this string into the below segment of "Node.js" inside the [Hadoop Zip File](https://github.com/Rhett-Ward/Lockalytics/blob/8e126bdee4f8e16646d5d8667e53285cb9bf8f33/src/Hadoop%20HDFS.zip)
      - <img width="900" height="300" alt="image" src="https://github.com/user-attachments/assets/5afeb1e8-8d5a-49f2-acfb-92613ce25b8c" />

### Step 5
Edit the directory of the virtual environment in the batch file

<img width="1016" height="53" alt="image" src="https://github.com/user-attachments/assets/b56d53cb-6369-47c0-9c5d-e2a02ee84cf8" />

### Step 6
Run the batch file!

Run it multiple times over a few minutes to see proof that the html file is actually pulling _id from your cluster

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

3/8/2026
Finished Initial implementation report. Did read me write up. Refactored batch file to run for anyone and not use specifics for my pc.

3/19/2026
Beginning full transitional process to automation and fully online hosting.
Purchased server to host the python script, the web app, the mongodb
Setup Azure blob storage to be the HDFS moving away from docker
Setup self hosted mongodb instance as opposed to using Atlas for size and cost reasons.
worked through lots of debugging. Got first set of raw data into Azure container.
got a sample express server up and running as a tester. 
started working on spark implementation in order to process data out of azure and into the self hosted mongodb instance.
started reworking python fetch script. to do: change dump location to azure blob rather then to mongo DB

4/4/2026
Completely debugged spark test pipeline thing. Was able to properly prove transfer into mongodb and from there into azure.
These issues included but not limited to: Running system python instead of venv python instance and therefore missing dependencies, Misspelled credentials, poor file search.



