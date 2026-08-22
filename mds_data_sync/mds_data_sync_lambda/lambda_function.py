import os
import json
from sqlalchemy import create_engine
import mysql.connector
from dotenv import load_dotenv
import logging
from mds_data_prep import mds_data_prep

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# https://docs.aws.amazon.com/lambda/latest/dg/python-package.html

def lambda_handler(event, context):

    # Pull data from MDS, and prepare for MySQL upload
    insert_df = mds_data_prep(local=False)

    # MySQL connection parameters
    # Load environment variables from .env file
    load_dotenv()

    # Accessing variables
    db_username = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_database = os.getenv('DB_NAME')
    table_name = os.getenv('TABLE_NAME')

    # Drop existing records
    connection = mysql.connector.connect(
        host=db_host,
        database=db_database,
        user=db_username,
        password=db_password
    )
    cursor = connection.cursor()
    # SQLAlchemy engine for MySQL
    engine_url = f'mysql+pymysql://{db_username}:{db_password}@{db_host}/{db_database}'
    engine = create_engine(engine_url)

    # Insert DataFrame into MySQL table
    try:
        insert_df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print("Success!")
    except sqlalchemy.exc.SQLAlchemyError as e:
        print(f'Unsuccessful insert. Error: {e}')

    # Update non-registered studies to have 0% completion
    try:
        cursor.execute(f"update {table_name} set overall_percent_complete='0' where is_registered ='not registered';")
        connection.commit()  # Commit the transaction to apply the changes
        print("unregistered studies zeroed out")
    except mysql.connector.Error as err:
        print("unsuccessful update, error:", err)


    # Update non-registered studies to have 0% completion
    try:
        cursor.execute(f"select appl_id, is_registered, overall_percent_complete  from progress_tracker where appl_id in ('10056337', '9608089', '9867358', '9900258', '10320676', '9839124', '10304570');")
        # Fetch the results
        rows = cursor.fetchall()

        # Get column names
        column_names = [column[0] for column in cursor.description]

        # Convert results to a list of dictionaries
        results = [dict(zip(column_names, row)) for row in rows]

        # Convert to JSON and pretty print
        print(json.dumps(results, indent=4))

    except mysql.connector.Error as err:
        print("unsuccessful update, error:", err)

    finally:
        cursor.close()

    response = {
        'statusCode': 200,
        'result': json.dumps(results, indent=4)
        }
    return response