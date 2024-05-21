import os
import json
import mysql.connector
from mysql.connector import errorcode
from faker import Faker


def lambda_handler(event, context):
    # Load sensitive data from environment variables
    db_config = {
        "user": os.getenv("DB_USERNAME"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
        "database": os.getenv("DB_NAME"),
    }

    # Create a Faker instance to generate fake data
    fake = Faker()

    try:
        # Connect to the MySQL database
        cnx = mysql.connector.connect(**db_config)
        cursor = cnx.cursor()

        # Show tables in the database
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print("Tables in the database:", tables)

        # Create table if it does not exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_table_query)
        print("Checked/Created table 'users'")

        # Insert a single record with fake data
        insert_query = """
        INSERT INTO users (name, email) VALUES (%s, %s)
        """
        fake_name = fake.name()
        fake_email = fake.email()
        cursor.execute(insert_query, (fake_name, fake_email))
        cnx.commit()
        print(f"Inserted record: Name={fake_name}, Email={fake_email}")

        # Close the cursor and connection
        cursor.close()
        cnx.close()

        return {
            "statusCode": 200,
            "body": json.dumps("Operation completed successfully."),
        }

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with the user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
        return {
            "statusCode": 500,
            "body": json.dumps("Failed to complete the operation."),
        }
    except Exception as e:
        print(e)
        return {"statusCode": 500, "body": json.dumps("An unexpected error occurred.")}
