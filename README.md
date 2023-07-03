# pgsql-unidb

Here

    uni_model0.py
    uni_model0_init.py

are used to create first database and to fill it with fake data. First
database has not table "groups".

After migration with help of alembic must appear the second database.
To migrate there is used

    uni_model_alembic.py

database model.

CRUD

    seed.py

without parameters is used to fill second database with fake data.
With parameters CRUD allows manage database state. Use

    seed.py --help

to print more information. Options are handled in oreder of their
appearences.

Scripts

    uni-select-??.py

contain some useful requests.
