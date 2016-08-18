#National Voter File
This project will build a database containing voter files for all fifty state. It uses a data model that represents a national voter files and associated campaign measures in a shared data warehouse. 

This model is based on standard data warehouse modelling techniques with an aim of making useful queries easy to generate while preserving history of the data. If you are unfamiliar with dimensional data modelling, you may enjoy [this article](https://dwbi.org/data-modelling/dimensional-model/1-dimensional-modeling-guide).

We assume that the movement app will maintain an Operational Data Store for transactional data related to administering and operating the application. This Online Analytic Processing database will perform the heavy lifting of historic analysis and voter identification.

##Design Objectives
This model was created with the following objectives in mind:

1. We will receive voter data from a variety of sources, not all of which are reliable
2. People change addresses
3. Electoral boundaries change over time
4. People live together in households
5. People change their names and genders

The model will be used for maintaining a national voter file that can be enriched with private data sources as well as crowd-sourced edits. Over time this voter file database will accumulate history of voter location and affiliation. 

Some of the data will be exposed as an open-source resource for any political campaign. An authorization model will be developed to allow campaigns to create private resources with data sharing between allied campaigns.

The voter file will be leveraged to power campaign donation, canvassing and other campaign interactions.

## Database Architecture
We have a document describing the data architecture available as a [google doc](https://docs.google.com/document/d/169mIkiIdl4OetbGvnbVCzq9Srw9PKCsB6U1CErTD9aI/edit?usp=sharing)

Take a look inside [SQL Folder](https://github.com/getmovement/national-voter-file/tree/master/src/main/sql) in the GitHub to see how to view and modify the relational model.

The database can be built and launched in one step using Docker. See the README inside /src/main/sql for instructions.

## How we work
We will use Pentaho's Opensource Extract Transform Load tool [Pentaho Data Integration](http://community.pentaho.com/projects/data-integration/) (also known as kettle) for the straightforward data load processes. More complex transformations may be coded in python as needed.

To learn how to install Pentaho data integration please refer to [this page](https://github.com/getmovement/national-voter-file/tree/master/src/tools)

Join our slack [#national-voter-file] (http://goo.gl/forms/8SJRDlo7Lx2rUsan1)

We have a [google doc](https://docs.google.com/spreadsheets/d/1CtNePb4LQSz-pk8UF58wwuVoBIc_YDAsBJZnIk7hKso/edit?usp=sharing) with information on obtaining each of the voter files

This site from  [The Election Project](http://voterlist.electproject.org/home) is a handy resource on each state's policies on obtaining and using their file.

We use Waffle.io to keep track of tasks that are ready for development:
[![Stories in Ready](https://badge.waffle.io/getmovement/national-voter-file.svg?label=ready&title=Ready)](http://waffle.io/getmovement/national-voter-file)
