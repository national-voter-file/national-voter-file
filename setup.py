from setuptools import find_packages, setup
import textwrap

packages = find_packages(where='./src/python')

setup(
    name='national_voter_file',
    version='0.1.1',
    author='xxxx',
    author_email='xxxx',
    packages=packages,
    package_dir=dict([(p, 'src/python/%s' % p.replace('.', '/')) for p in packages]),
    #package_data={'national_voter_file.tests': ['*.py']},
    #include_package_data=True,
    url='https://github.com/national-voter-file/national-voter-file',
    license='MIT',
    test_suite='nose.collector',
    tests_require=[
        'nose',
        'Faker',
        'future',
        'probableparsing',
        'python-crfsuite',
        'python-dateutil',
        'six',
        'usaddress',
    ],
    description="",
    long_description="""
    """,
    entry_points={
        'console_scripts': [
            'process_voter_files = national_voter_file.transformers.csv_transformer:main',
        ],
    },
    install_requires=[
        'requests',
        'usaddress',
    ],
    keywords = "scraping politics united_states voters",
    classifiers=['Development Status :: 4 - Beta', 'Environment :: Console', 'Intended Audience :: Developers', 'Natural Language :: English', 'Operating System :: OS Independent', 'Topic :: Text Processing'],
)
