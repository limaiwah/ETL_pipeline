import psycopg2
import pandas as pd


def connect_to_redshift(dbname, host, port, user, password):
    """Method that connects to redshift. This gives a warning so will look for another solution"""

    connect = psycopg2.connect(
        dbname=dbname, host=host, port=port, user=user, password=password
    )

    print("connection to redshift made")

    return connect

# create definition to connect to redshift and carries out the following transformation tasks

def extract_transactional_data(dbname, host, port, user, password):
    """
        This function connects to redshift and carries out the following transformation tasks
        1. Removes all rows where customer id is missing
        2. Removes the stock codes M, D, CRUK, POST, BANK CHARGES,
        3. Adds description to the online transactions table
        4. Replaces missing stock description with Unknown
        5. Fixes the data type of invoice date
        6. Create a new variable called total order value
        """

    # connect to redshift
    connect = connect_to_redshift(dbname, host, port, user, password)

    # query to extract online transactions data

    query = """
    SELECT ot.invoice,
           ot.stock_code,
           CASE WHEN s.description is null then 'Unknown'
                ELSE s.description END as description,
           ot.price,
           ot.quantity,
           /* add variable of total_order_value*/
           ot.price*ot.quantity AS total_order_value,
           CAST(invoice_date as DateTime) AS invoice_date,
           ot.customer_id,
           ot.country
    FROM bootcamp.online_transactions ot
    LEFT JOIN (SELECT * /*subqueries to not include '?' description*/
               FROM bootcamp.stock_description sd 
               WHERE description <> '?') AS s ON ot.stock_code = s.stock_code
    WHERE ot.customer_id <> '' AND 
          ot.stock_code NOT IN ('BANK CHARGES', 'POST', 'D', 'M', 'CRUK') 
        """

    online_trans_cleaned = pd.read_sql(query, connect)

    print("Online trans clean shape", online_trans_cleaned.shape)
    print("Online transactions head", online_trans_cleaned.head())

    return online_trans_cleaned
