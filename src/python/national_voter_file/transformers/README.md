# Adding a new state

Here are the basic steps you want to perform:

* copy `state_transformer_template.py` to `us_states/{state_postal_initials}/transfomer.py` (e.g. `us_states/pa/transformer.py`)
* Update the values in the headers
  1. `state_path` - this is the two letter state code (e.g. tx)
  2. `state_name` - CamelCase state namme (e.g. NewYork)
  3. `sep` - Delimiter between columns
  4. `date_format` - The python date parser format string for how to read dates for this voter file.
  5. `input_fields` - a list of column names for the source file. This should be set to `None` when the file has a header row.

* The next step is to add tests in the following places:
** `src/test/python/test_transformers.py` (with a `test_{state initials}_transformer()` method, ideally in alphabetical order)
** `src/test/python/faker_data.py` (add in your state)
** `src/test/python/test_data/{STATE NAME}.csv`
* And then include file info documentation if there was any that came with the data: in `docs/state-voter-file-data-descriptions/`

# How to run test suite
We use [Nosetest](http://nose.readthedocs.io/en/latest/) to run automated tests on our files. At the moment these tests are rudimentry, but at least verify basic functionality of the transformers:

```nosetests src/python/national_voter_file/tests/test_transformers.py```

To run it using a specific version of Python:

```python3.5 -m nose src/python/national_voter_file/tests/test_transformers.py```

# How to Invoke transformers
We now have a standard script to run any state. It takes arguments to speifcy the state code, and the input and output directories:

  ``` python3.5 national_voter_file/transformers/csv_transformer.py
 -s ny -o ../../data/NewYork -d ../../data/NewYork```
