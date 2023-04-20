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
        return fake.date_of_birth(minimum_age=18, maximum_age=90)
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


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args,dbt_load_df_function):
    refs = {}
    key = '.'.join(args)
    return dbt_load_df_function(refs[key])


def source(*args, dbt_load_df_function):
    sources = {}
    key = '.'.join(args)
    return dbt_load_df_function(sources[key])


config_dict = {'nb_clients': 70}


class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

class this:
    """dbt.this() or dbt.this.identifier"""
    database = "PERSO"
    schema = "ARO"
    identifier = "add_clients"
    
    def __repr__(self):
        return 'PERSO.ARO.add_clients'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args: ref(*args, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------

# To run this in snowsight, you need to select entry point to be main
# And you may have to modify the return type to text to get the result back
# def main(session):
#     dbt = dbtObj(session.table)
#     df = model(dbt, session)
#     return df.collect()

# to run this in local notebook, you need to create a session following examples https://github.com/Snowflake-Labs/sfguide-getting-started-snowpark-python
# then you can do the following to run model
# dbt = dbtObj(session.table)
# df = model(dbt, session)

