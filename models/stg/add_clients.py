import snowflake.snowpark.functions as F
from faker import Faker


def add_clients(session, nb):
    clients = []
    for _ in range(nb):
        clients.append([fake('id'), fake('first_name'), fake('last_name'), fake('birthdate')])
    df = session.create_dataframe(clients, schema=["ID", "FIRST_NAME", "LAST_NAME", "BIRTHDATE"])
    return df
def fake(a):
    fake = Faker()
    if a=='first_name':
        return fake.first_name()
    if a=='last_name':
        return fake.last_name()
    if a=='birthdate':
        return fake.date_of_birth(minimum_age=30, maximum_age=92)
    if a=='id':
        return fake.pyint()
    else:
        return 'other'

def model(dbt, session):
    dbt.config(
        packages = ["Faker"]
    )
    dbt.config(materialized = "incremental")
    nb_clients = dbt.config.get("nb_clients")
    return add_clients(session, nb_clients)