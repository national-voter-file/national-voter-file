# Adding a new state

* Create a new transformer from an existing one:

```cp -R us_states/mi us_states/{new_state_postal_initials}```

* Update it accordingly. Besides updating the methods, you'll need to update these variables:

  1. `state_path` - this is the two letter state code (e.g. tx)
  2. `state_name` - CamelCase state namme (e.g. NewYork)
  3. `sep` - Delimiter between columns
  4. `date_format` - The python date parser format string for how to read dates for this voter file.
  5. `col_map` - A mapping of column names that don't need to be transformed in any way.
  6. `input_fields` - a list of column names for the source file. This should be set to `None` when the file has a header row.

* The next step is to add tests in the following places:

** `src/test/python/test_transformers.py` (with a `test_{state initials}_transformer()` method, ideally in alphabetical order)
** `src/test/python/faker_data.py` (add in your state)
** `src/test/python/test_data/{new_state_postal_initials}.csv`

* And then include file info documentation if there was any that came with the data, in `docs/state-voter-file-data-descriptions/`

# Generating fake data

For the transformer tests, a fake data file is required. The transformers test runs the transformer on this file. It lives at
`src/test/python/test_data/{new_state_postal_initials}.csv` and is generated with the following command:

```
python3 national_voter_file/tests/faker_data.py {new_state_postal_initials}
```

# Running tests

We use [Nosetest](http://nose.readthedocs.io/en/latest/) to run automated tests on our files. At the moment these tests are rudimentry, but at least verify basic functionality of the transformers:

```nosetests src/python/national_voter_file/tests/test_transformers.py```

To run it using a specific version of Python:

```python3.5 -m nose src/python/national_voter_file/tests/test_transformers.py```

Make sure your code also passes our linter:

```pylint --rcfile=.pylintrc src/python/national_voter_file/us_states/ -f parseable -r n```

# How to Invoke transformers

We now have a standard script to run any state. It takes arguments to specify the state code, and the input and output directories:

  ``` python3.5 national_voter_file/transformers/csv_transformer.py
 -s ny -o ../../data/NewYork -d ../../data/NewYork```

# Tips on running the python code

## Installing Dependencies

Dependencies are installed via the `requirements.txt` file in this folder.

```
pip install -r requirements.txt
```

It's recommended to use [Virtualenv](https://virtualenv.pypa.io/en/stable/) for managing these.

## Common errors

If you see the error:

```
ImportError: No module named 'national_voter_file'
```

come up when you run any of these python scripts, note that you might need to set the `PYTHONPATH`
environment variable. For the transformer scripts, it should be set to `national_voter_file/src/python`.
