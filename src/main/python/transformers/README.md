==Adding a new state

Here are the basic steps you want to perform:

* copy `state_transformer_template.py` to `{state_postal_initials}.py` (e.g. `pa.py`)
* include an `if __name__ == "__main__": ...` line that accepts an input and output file argument
** other arguments can be made available, but all others should have defaults based on `DATA_DIR` (from src.main.python.transformers __init__.py).  Defaults should be based on the names of the files provided by the state.
* The next step is to add tests in the following places:
** `src/test/python/test_transformers.py` (with a `test_{state initials}_transformer()` method, ideally in alphabetical order)
** `src/test/python/faker_data.py` (add in your state)
** `src/test/python/test_data/{STATE NAME}.csv`
* And then include file info documentation if there was any that came with the data: in `docs/state-voter-file-data-descriptions/`

