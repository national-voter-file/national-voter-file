# National Voter File

We provide an easy-to-use, modern-era database with voter files for each of the fifty states. It uses a data model that represents a national voter file as well as associated campaign measures in a shared data warehouse.

We want to pull politics into the 21st century, and we're starting from the ground up.

## Our goals

* Reliable, up-to-date voter data for every state in the country (including address changes, and redrawn districts)
* Per-household insights, done by grouping voters together based on address
* Per-voter insights, such as when people change their names and genders

* And for thousands to use our voter file to power their campaigns' donation, canvassing, and phonebanking efforts!

## How can I help?

* First, join us on [Slack](http://goo.gl/forms/8SJRDlo7Lx2rUsan1).
* Take a look at [our todo list](http://waffle.io/getmovement/national-voter-file).
* Learn about [the code](https://github.com/getmovement/national-voter-file/tree/master/docker) itself!

Here's the bulk of the work:

1. [Collect voter files for every state](https://trello.com/b/IlZkwYc0/national-voter-file-states-pipeline) and store them securely.
2. Write state-specific Python scripts to turn them into a standardized input file (SIF).
3. Make sure that data is clean and consistent nationally.
4. Load that data into a queryable database using Pentaho.
5. Build a simple, accessible, easy-to-vend API for consumers.

## Often asked

*How are we supposed to get all the voter files we need?*

You're in luck. We have a [Google document](https://docs.google.com/spreadsheets/d/1CtNePb4LQSz-pk8UF58wwuVoBIc_YDAsBJZnIk7hKso/edit?usp=sharing) with information on how to catch them all.

*Is this legal?!*

[The Election Project](http://voterlist.electproject.org/home) is a handy resource on every state's policies regarding their voter file.
