from mysql.connector import Error
import mysql.connector
from data_random import data
import os

connection = None

try:
    connection = mysql.connector.connect(
        host=os.environ.get('DB_HOST', 'localhost'),
        port=int(os.environ.get('DB_PORT', 3306)),
        user=os.environ.get('DB_USER', 'root'),
        password=os.environ.get('DB_PASSWORD', ''),
        database=os.environ.get('DB_NAME', 'mi_db_l')
    )

    if connection.is_connected():
        cursor = connection.cursor()
        cursor.executemany(
            """INSERT INTO usuarios(
                name,
                company,
                job,
                email,
                phone,
                mac_address
            ) VALUES (%s, %s, %s, %s, %s, %s)""",
            data
        )

        if len(data) == cursor.rowcount:
            connection.commit()
            print("{} Filas insertadas.".format(len(data)))
        else:
            connection.rollback()
except Error as ex:
    print("Error en la conexion: {}".format(ex))
finally:
    if connection and connection.is_connected():
        connection.close()
        print("Conexion cerrada.")
