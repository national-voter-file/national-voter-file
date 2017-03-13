[![Slack](https://nationalvoterfileslackin.herokuapp.com/badge.svg)](https://nationalvoterfileslackin.herokuapp.com) [![Build Status](https://travis-ci.org/national-voter-file/national-voter-file.svg?branch=master)](https://travis-ci.org/national-voter-file/national-voter-file)

# National Voter File

We provide an easy-to-use, modern-era database with voter files for each of the fifty states. It uses a data model that represents a national voter file as well as associated campaign measures in a shared data warehouse.

We want to pull politics into the 21st century, and we're starting from the ground up.

### Goals

* Reliable, up-to-date voter data for every state in the country (including address changes, and redrawn districts)
* All available via an easy-to-consume REST API
* For thousands to use our voter file to power their campaigns' donation, canvassing, and phonebanking efforts!

## What about privacy?

We realize that voter files contain a lot of sensitive information. That's why we
will make sure that organizations that use the voter file are candidates or
politically oriented nonprofits that would have a reason to organize voters--not
businesses and individuals trying to profit or use your personal information for
malicious purposes.

The problem we're trying to solve is that the two major political parties each
control their own versions of this information that grassroots candidates without
their approval can't access easily. We want to make sure that these candidates
and organizations who want to use it for appropriate purposes are able to.

## Will this data be publicly available?

No. The access will be restricted to the political candidates and issue-based nonprofits,
and will not be publicly available on search engines.

## How does it work?

Glad you asked! Simple, we:

### 1. Collect voter files from every state

We've done nine states so far. We'd [love your help collecting them all](https://trello.com/b/IlZkwYc0/national-voter-file-states-pipeline).

### 2. Extract and transform that data

Each state does it differently, some (way) worse than others. Using well-tested, state-specific [transformer scripts](src/python/national_voter_file/transformers/README.md) written in Python, we turn them into consistent CSV files.

### 3. Load that data into a Postgres database

We load the data using [Pentaho](load/README.md), and contain the database and its query layer [within Docker](docker/README.md) so that it is platform agnostic.

### 4. Build [a simple, accessible, easy-to-vend API](https://github.com/national-voter-file/national-voter-file-api) for consumers.

We haven't started on this yet. It's coming soon!

## This sounds awesome! How can I help?

* We chat on Slack. [Join us!](https://nationalvoterfileslackin.herokuapp.com/)
* Get a local development environment using Docker, [Docker setup instructions](docker/README.md)
* Take a look at our [newcomer issues](https://github.com/national-voter-file/national-voter-file/projects/1) to see where you can help.
