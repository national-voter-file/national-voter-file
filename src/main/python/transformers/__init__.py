from os.path import dirname
import os

DATA_DIR = os.path.join(
    dirname(dirname(dirname(dirname(dirname(__file__))))), 'data'
)
