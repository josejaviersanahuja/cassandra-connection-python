from database import *
import csv
from datetime import datetime

if __name__ == "__main__":
    try:
        # Creating session object
        session = create_session()

        # Keyspace to be used for performing various operations
        KEYSPACE = "testkeyspace"

        # Setting Keyspace in the cassandra session
        execute_query(session, "CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE +
                      " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        # Code for setting session with cassandra database
        set_session_keyspace(session, KEYSPACE)
        # Creating table 'Customer'
        create_customer_table_query = 'CREATE TABLE IF NOT EXISTS Customer (cust_id text PRIMARY KEY, first_name text, last_name text, registered_on timestamp)'

        if execute_query(session, create_customer_table_query) is not None:
            print("Customer table is successfully created.")

        # Creating table 'Product'
        create_product_table_query = 'CREATE TABLE IF NOT EXISTS Product (prdt_id text PRIMARY KEY, title text)'

        if execute_query(session, create_product_table_query) is not None:
            print("Product table is successfully created.")

        # Creating table 'Product_Liked_By_Customer'
        create_product_liked_by_customer_table_query = 'CREATE TABLE IF NOT EXISTS Product_Liked_By_Customer (cust_id text, first_name text, last_name text, liked_prdt_id text, liked_on timestamp, title text, PRIMARY KEY (liked_prdt_id, cust_id))'

        if execute_query(session, create_product_liked_by_customer_table_query) is not None:
            print("Product_Liked_By_Customer table is successfully created.")

        # Inserting Customer data in the table
#        customer_data_insert_query = "INSERT INTO Customer (cust_id, first_name, last_name, registered_on) VALUES ('%s', '%s', '%s', %s)"
#        with open("config/customers.csv", "r") as file:
#            csvreader = csv.reader(file)
#            header = next(csvreader)
#            for row in csvreader:
#                execute_query(session, customer_data_insert_query % (
#                    row[0],
#                    row[1],
#                    row[2],
#                    int(float(datetime.now().strftime("%s.%f"))) * 1000))
#
#        # Inserting Product data in the table
#        product_data_insert_query = "INSERT INTO Product (prdt_id, title) VALUES ('%s', '%s')"
#        with open("config/products.csv", "r") as file:
#            csvreader = csv.reader(file)
#            header = next(csvreader)
#            for row in csvreader:
#                execute_query(session, product_data_insert_query % (
#                    row[0],
#                    row[1]))
#
        # Inserting Product_Liked_By_Customer data in the table
        product_liked_by_customer_data_insert_query = "INSERT INTO Product_Liked_By_Customer (cust_id, first_name, last_name, liked_prdt_id, liked_on, title) VALUES ('%s', '%s', '%s', '%s', %s, '%s')"
        with open("config/product_liked_by_customer.csv", "r") as file:
            csvreader = csv.reader(file)
            header = next(csvreader)
            for row in csvreader:
                execute_query(session, product_liked_by_customer_data_insert_query % (
                    # print(product_liked_by_customer_data_insert_query % (
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    int(float(datetime.now().strftime("%s.%f"))) * 1000,
                    row[4]))

    except Exception as e:
        print("Error in the execution : ", str(e))
