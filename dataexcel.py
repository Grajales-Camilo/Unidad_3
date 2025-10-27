import pandas as pd
from mysql.connector import Error
import mysql.connector
import os
import argparse

parser = argparse.ArgumentParser(description='Import Excel data to MySQL.')
parser.add_argument('--excel', type=str, default='data1.xlsx', help='Path to the Excel file')
args = parser.parse_args()

df = pd.read_excel(args.excel, sheet_name='Data')

# Descarta columnas que no se cargarán en la tabla destino.
df = df.drop(columns=['Country', 'City'])
filas = []
for _, row in df.iterrows():
    filas.append(row.tolist())

connection = None
try:
    connection = mysql.connector.connect(
        host=os.environ.get('DB_HOST', 'localhost'),
        port=int(os.environ.get('DB_PORT', 3306)),
        user=os.environ.get('DB_USER', 'root'),
        password=os.environ.get('DB_PASSWORD'),
        database=os.environ.get('DB_NAME', 'mi_db_l')
            """INSERT INTO empleados(
                employee_id,
                full_name,
                job_title,
                department,
                business_unit,
                gender,
                ethnicity,
                age,
                annual_salary,
                bonus
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            filas
                Age,
                Annual_Salary,
                Bonus
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            filas
        )
        if len(filas) == cursor.rowcount:
            connection.commit()
            print("{} Filas insertadas.".format(len(filas)))
        else:
            connection.rollback()
except Error as ex:
    print("Error en la conexion: {}".format(ex))
finally:
    if connection and connection.is_connected():
        connection.close()
        print("Conexion cerrada.")
