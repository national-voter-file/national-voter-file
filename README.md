# National Voter File

We provide an easy-to-use, modern-era database with voter files for each of the fifty states. It uses a data model that represents a national voter file as well as associated campaign measures in a shared data warehouse.

We want to pull politics into the 21st century, and we're starting from the ground up.

## Our goals

* Reliable, up-to-date voter data for every state in the country (including address changes, and redrawn districts)
* Per-household insights, done by grouping voters together based on address
* Per-voter insights, such as when people change their party affiliation or address
* And for thousands to use our voter file to power their campaigns' donation, canvassing, and phonebanking efforts!

## How can I help?

* We chat on Slack. [Join us!](http://goo.gl/forms/8SJRDlo7Lx2rUsan1)
* Take a look at our [Issues](http://waffle.io/getmovement/national-voter-file) to see where you can fit in.

Here's the bulk of the work:

1. [Collect voter files for every state](https://trello.com/b/IlZkwYc0/national-voter-file-states-pipeline) and store them securely.
2. Write [state-specific Python scripts](https://github.com/national-voter-file/national-voter-file/blob/master/src/main/python/NewYorkPrepare.py) to turn them into a [standardized input file](https://docs.google.com/spreadsheets/d/e/2PACX-1vTkv4Js43Wl_I0mpqt7FnFMt1pOTy1GwTZfPCW--TufdUzepSEHBAxsQTV3Ic_u9t5TY28OqSy-I28L/pubhtml).
3. Make sure that [data is clean and consistent](https://github.com/national-voter-file/national-voter-file/issues/56) nationally.
4. Load that data into [a queryable database](https://github.com/national-voter-file/national-voter-file/tree/master/docker) using [Pentaho](https://github.com/national-voter-file/national-voter-file/blob/master/tools/README.md).
5. Build [a simple, accessible, easy-to-vend API](https://github.com/national-voter-file/national-voter-file-api) for consumers.
