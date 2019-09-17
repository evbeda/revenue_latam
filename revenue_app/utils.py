import pandas as pd


# transactions_w_pp = pd.read_excel('datasets/Revenue Queries.xlsx', sheet_name=1, header=1, usecols=range(0,16))
# transactions_w_email = pd.read_excel('datasets/Revenue Queries.xlsx', sheet_name=2, header=1, usecols=range(0,16))

def get_rollups():
    return pd.read_excel('datasets/Revenue Queries.xlsx', sheet_name=0, header=1, usecols=range(0,53))


def get_transactions():
    return pd.read_csv('datasets/transactions.csv')


def get_organizer_sales():
    return pd.read_csv('datasets/organizer_sales.csv')


def get_organizer_list():
    return []
