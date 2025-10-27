from mysql.connector import Error
import mysql.connector
import os


def run_query(cursor, query, params=None, fetch=False):
    cursor.execute(query, params or ())
    if fetch:
        return cursor.fetchall()
    return None


connection = None
def demo_db_manipulation(commit_changes=False):
    connection = None
    try:
        connection = mysql.connector.connect(
            host='localhost',
            port=3306,
            user='root',
            password='Genius2.4005',
            db='mi_db_l'
        )

        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
            connection.start_transaction()

            total_empleados = run_query(
                cursor,
                "SELECT COUNT(*) AS total FROM empleados",
                fetch=True
            )
            print("Total de empleados:", total_empleados[0]["total"])

            run_query(
                cursor,
                "UPDATE usuarios SET job = %s WHERE name = %s",
                ("Senior Data Analyst", "Alice Johnson")
            )
            updated_usuario = run_query(
                cursor,
                "SELECT name, job FROM usuarios WHERE name = %s",
                ("Alice Johnson",),
                fetch=True
            )
            print("Usuario actualizado:", updated_usuario)

            run_query(
                cursor,
                "INSERT INTO usuarios(name, company, job, email, phone, mac_address) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (
                    "Usuario Temporal",
                    "Demo Corp",
                    "Tester",
                    "temporal@example.com",
                    "+1-999-000-0000",
                    "AA:BB:CC:DD:EE:FF",
                ),
            )
            nuevos_usuarios = run_query(
                cursor,
                "SELECT name FROM usuarios ORDER BY id DESC LIMIT 3",
                fetch=True
            )
            print("Últimos usuarios insertados:", nuevos_usuarios)

            run_query(
                cursor,
                "DELETE FROM usuarios WHERE name = %s",
                ("Usuario Temporal",)
            )

            if commit_changes:
                connection.commit()
                print("Cambios confirmados en la base de datos.")
            else:
                connection.rollback()
                print("Cambios revertidos para mantener la tabla estable.")

    except Error as ex:
        print("Error al manipular la base de datos:", ex)
    finally:
        if connection and connection.is_connected():
            connection.close()
            print("Conexion cerrada.")

# Llama a la función con commit_changes=True para confirmar cambios, False para revertir
demo_db_manipulation(commit_changes=False)
        print("Conexion cerrada.")
