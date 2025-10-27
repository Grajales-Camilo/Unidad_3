from mysql.connector import Error
import mysql.connector
import os


DDL_STATEMENTS = [
    "CREATE DATABASE IF NOT EXISTS mi_db_l CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
    "USE mi_db_l",
    """
    CREATE TABLE IF NOT EXISTS empleados (
        Employee_ID VARCHAR(20) PRIMARY KEY,
        Full_Name VARCHAR(150),
        Job_Title VARCHAR(120),
        Department VARCHAR(120),
        Business_Unit VARCHAR(120),
        Gender VARCHAR(20),
        Ethnicity VARCHAR(50),
        Age INT,
        Annual_Salary DECIMAL(12,2),
        Bonus DECIMAL(12,2)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS usuarios (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(120),
        company VARCHAR(120),
        job VARCHAR(120),
        email VARCHAR(150),
        phone VARCHAR(40),
        mac_address VARCHAR(17)
    )
    """,
]


def run():
    connection = None
    try:
        connection = mysql.connector.connect(
            host=os.environ.get("DB_HOST", "localhost"),
            port=int(os.environ.get("DB_PORT", 3306)),
            user=os.environ.get("DB_USER", "root"),
            password=os.environ.get("DB_PASSWORD"),
        )
        cursor = connection.cursor()
        for statement in DDL_STATEMENTS:
            cursor.execute(statement)
        connection.commit()
        print("Base de datos y tablas listas.")
    except Error as ex:
        print("Error configurando la base de datos:", ex)
    finally:
        if connection and connection.is_connected():
            connection.close()
            print("Conexion cerrada.")


if __name__ == "__main__":
    run()
