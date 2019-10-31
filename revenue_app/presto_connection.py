import pandas as pd
from pyhive import presto
from pyhive.exc import DatabaseError


class PrestoError(Exception):
    def __init__(self, error_message):
        self.args = (self.get_message(error_message),)

    def get_message(self, error_message):
        if 'NewConnectionError' in error_message:
            # Raised when not connected to the VPN
            message = error_message.split('>:')[1].split("'))")[0].strip()
            message += '.<br>Make sure you are connected to the VPN.'
        elif 'SSLCertVerificationError' in error_message:
            # Raised when certificate not in env var
            message = error_message.split("(1, '")[1].split("')))")[0].strip()
            message += '.<br>Update your certificate '
            message += '(<a href="https://docs.evbhome.com/intro/self_signed_certs.html">link</a>).'
        elif ('Invalid credentials' in error_message) or ('Malformed decoded credentials' in error_message):
            # Raised when wrong or empty okta username/password
            message = error_message.split('<pre>')[1].split('</pre>')[0].strip()
            message += '.<br>Check your okta username and password.'
        else:
            message = error_message
        return message


def read_sql(file_name):
    with open('sql/{}.sql'.format(file_name), 'r') as fd:
        sql_file = fd.read()
    return sql_file


def query_presto(start_date, end_date, okta_username, okta_password, query, query_name):
    try:
        connection = presto.connect(
            'presto-tableau.prod.dataf.eb',
            8443,
            okta_username,
            password=okta_password,
            protocol='https',
        )
        dataframe = pd.read_sql(query.format(start_date, end_date), connection)
        connection.close()
    except pd.io.sql.DatabaseError as exception:
        error = exception.args[0].split('\n\n')[1]
        raise PrestoError(error)
    except DatabaseError as exception:
        # Raised when you don't have permissions to a specific table
        # TODO: this 'if' should dissapear once we know what to do with the transactions query
        if query_name == 'transactions':
            dataframe = pd.read_csv('datasets/transactions.csv')
            return dataframe
        message = exception.args[0]['message']
        raise PrestoError(message)
        # TODO: call method that deletes (rollbacks) the other queries if done
    except Exception as exception:
        message = "Unknown error.<br>" + str(exception)
        raise PrestoError(message)
    return dataframe


def make_query(start_date, end_date, okta_username, okta_password, query_name):
    query = read_sql(query_name)
    dataframe = query_presto(start_date, end_date, okta_username, okta_password, query, query_name)
    return dataframe
