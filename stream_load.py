from dotenv import load_dotenv
import os
import mysql.connector as mysql

load_dotenv()

header = os.getenv("FILE_HEADER")
mysql_host = os.getenv("DB_HOST")
mysql_port = os.getenv("DB_PORT")
mysql_db = os.getenv("DB_NAME")
mysql_table = "vehicles"
mysql_user = os.getenv("DB_USER")
mysql_password = os.getenv("DB_PASSWORD")
filename = os.getenv("FILE_NAME")
num_inserts = 1


def db_connect():
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
    except Exception as e:
        print("an error occurred", e)


def stream_read_load():
    with open(filename, "r") as file:
        header = "`" + file.readline().replace("\n", "").replace(",", "`,`") + "`"
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

        print("insert completed")


def insert_into_table(lines, header):
    sql = """INSERT INTO %s (%s) VALUES %s """ % (mysql_table, header, lines)

    print("insert number: ", num_inserts)

    num_inserts += 1

    try:
        with db_connect() as db:
            cursor = db.cursor()

            cursor.execute(sql)

            db.commit()

    except Exception as e:
        print("error occurred inserting", e)


stream_read_load()
