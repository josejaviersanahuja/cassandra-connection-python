from cassandra.cluster import Cluster
# from cassandra.auth import PlainTextAuthProvider

# AstraDB Client and Secret required for the connection
# ASTRA_CLIENT_ID = None
# ASTRA_SECRET = None

# Cloud Configuration
# cloud_config= {
#         'secure_connect_bundle': 'secure-connect-<your_bundle>.zip'
# }

# Authentication Provider
# auth_provider = PlainTextAuthProvider(username=ASTRA_CLIENT_ID, password=ASTRA_SECRET)

# session object whuch is used fot interacting with Cassandra database
#session = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = Cluster(contact_points=['127.0.0.1'], port=9042)

# Method for creating session object based on cloud configuration and Auth Provider


def create_session():
    return session.connect()

# Method for setting keyspace in the session
# Input Parameters:
#    Session object
#    Keyspace_name


def set_session_keyspace(session, keyspace_name):
    session.set_keyspace(keyspace_name)

# Method for executing query using the given session object
# Input Parameters:
#    Session object
#    query for execution


def execute_query(session, query):
    return session.execute(query)

# Method for executing single row query using the given session object
# Input Parameters:
#    Session object
#    query for execution


def execute_single_row_query(session, query):
    return session.execute(query).one()

# Method for fetching table data using the given session object
# Input Parameters:
#    Session object
#    table name for which the data is to be fetched


def show_table_data(session, table_name):
    return session.execute("SELECT * FROM " + table_name).all()

# Method for closing the session object
# Input Parameters:
#    Session object


def close_session(session):
    session.shutdown()


if __name__ == "__main__":
    try:
        session = create_session()
        KEYSPACE = "testkeyspace"
        execute_query(session, "CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE +
                      " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        set_session_keyspace(session, KEYSPACE)

        create_customer_table_query = 'CREATE TABLE IF NOT EXISTS Customer ("Customer_ID" int PRIMARY KEY, "First_Name" text, "Last_Name" text)'

        if execute_query(session, create_customer_table_query) is not None:
            print("Customer table is successfully created.")

        close_session(session)
    except Exception as e:
        print("Error in the execution : ", str(e))
