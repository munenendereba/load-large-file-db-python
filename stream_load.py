"""
this module simulates reading a large file 
and then parsing it line by line and inserting it into db
"""

import os
from dotenv import load_dotenv
import mysql.connector as mysql

load_dotenv()


mysql_host = os.getenv("DB_HOST")
mysql_port = os.getenv("DB_PORT")
mysql_db = os.getenv("DB_NAME")
MYSQL_TABLE = "vehicles"
mysql_user = os.getenv("DB_USER")
mysql_password = os.getenv("DB_PASSWORD")
filename = os.getenv("FILE_NAME")


def db_connect():
    """method to connect to db"""
    try:
        db = mysql.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            database=mysql_db,
            port=mysql_port,
            allow_local_infile=True,
        )

        print("connected to db")

        return db
    except mysql.Error() as e:
        print("an error occurred", e)

        return None


def stream_read_load():
    """open the file, read it, loop the lines and insert into db"""

    with open(filename, "r", encoding="UTF-8") as file:
        header = "`" + file.readline().replace("\n", "").replace(",", "`,`") + "`"

        num_inserts = 1
        print(header)
        next(file)
        lines = ""
        num = 0
        for line in file:
            line = '("' + line.replace(",", '","') + '")'

            lines += line + ","
            num += 1
            if num >= 50000:
                lines = lines.rstrip(",")
                insert_into_table(lines, header)
                num = 0
                lines = ""

                print("batch: ", num_inserts)

                num_inserts += 1

        print("processing complete")


def insert_into_table(lines, header):
    """takes in lines and header
    returns nothing
    """
    sql = f"""INSERT INTO {MYSQL_TABLE} ({header}) VALUES {lines} """

    try:
        with db_connect() as db:
            cursor = db.cursor()

            cursor.execute(sql)

            db.commit()

    except mysql.Error() as e:
        print("error occurred inserting", e)


stream_read_load()
