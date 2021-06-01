import psycopg2

def get_connection():
    connection = psycopg2.connect(user="wibidxcc",
                                  password="z9HmQSugSMrNzkHtrYr4q4SR7B5IRXwU",
                                  host="batyr.db.elephantsql.com",
                                  port="5432",
                                  database="wibidxcc")
    return connection

def close_connection(connection):
    if connection:
        connection.close()
