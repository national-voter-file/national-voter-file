![National Voter File](https://d3vv6lp55qjaqc.cloudfront.net/items/231s0n30283j2W2S0u1L/NVF%20small.png?X-CloudApp-Visitor-Id=1336043&v=dfe5cf15)

# National Voter File

We provide an easy-to-use, modern-era database with voter files for each of the fifty states. It uses a data model that represents a national voter file as well as associated campaign measures in a shared data warehouse.

We want to pull politics into the 21st century, and we're starting from the ground up.

## Our goals

* Reliable, up-to-date voter data for every state in the country (including address changes, and redrawn districts)
* Per-household insights, done by grouping voters together based on address
* Per-voter insights, such as when people change their names and genders

* And for thousands to use our voter file to power their campaigns' donation, canvassing, and phonebanking efforts!

## About the architecture

This model is based on standard data warehouse modelling techniques with an aim of making useful queries easy to generate while preserving history of the data. If you are unfamiliar with dimensional data modelling, you may enjoy [this article](https://dwbi.org/data-modelling/dimensional-model/1-dimensional-modeling-guide).

We assume that the movement app will maintain an Operational Data Store for transactional data related to administering and operating the application. This Online Analytic Processing database will perform the heavy lifting of historic analysis and voter identification.

We have a document describing the data architecture available as a [Google document](https://docs.google.com/document/d/169mIkiIdl4OetbGvnbVCzq9Srw9PKCsB6U1CErTD9aI/edit?usp=sharing).

Take a look inside [the SQL folder](https://github.com/getmovement/national-voter-file/tree/master/src/main/sql) to see how you can view and modify the relational model.

## How we get things done

* We write in Python. See a list of the tools we use [here](https://github.com/getmovement/national-voter-file/tree/master/src/tools).
* We are on [#Slack](http://goo.gl/forms/8SJRDlo7Lx2rUsan1)!
* We use Waffle.io (built on top of Issues) to stay on top of stuff. [Join us!](http://waffle.io/getmovement/national-voter-file)

## FAQ

*How are we supposed to get all the voter files we need?*

You're in luck. We have a [Google document](https://docs.google.com/spreadsheets/d/1CtNePb4LQSz-pk8UF58wwuVoBIc_YDAsBJZnIk7hKso/edit?usp=sharing) with information on how to obtain each voter file we need.

*Is this legal?!*

[The Election Project](http://voterlist.electproject.org/home) is a handy resource on every state's policies regarding their voter file.
