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
