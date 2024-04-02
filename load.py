"""
loading a large .csv file using
mysql load data in file
"""

# import the mysql library
import mysql.connector as mysql

DATA_FILE = r"D:\\WorkManenos\\NextPhase\\resources\\car_prices.csv"
DELIMITER = ","
NEWLINE = "\n"
HEADER = True
MYSQL_HOST = "localhost"
MYSQL_PORT = 3308
MYSQL_DB = "car_prices"
MYSQL_TABLE = "vehicles"
MYSQL_USER = "root"
MYSQL_PASSWORD = ""


# ----------------------------------
# read first line of the input file to create header

with open(DATA_FILE, "r", encoding="UTF-8") as inFile:
    HEADER = inFile.readline()
    inFile.close()
    # ---------------------------------------------------
    # Extract column lists
    column_list = HEADER.split(DELIMITER)
    # ---------------------------------------------------
    # Construct Create table statement
    CREATE_TABLE_STATEMENT = "CREATE TABLE IF NOT EXISTS " + MYSQL_TABLE + "("
    for a_field in column_list:
        CREATE_TABLE_STATEMENT += "`" + a_field.replace("\n", "") + "`" + " text,"
    CREATE_TABLE_STATEMENT = CREATE_TABLE_STATEMENT[:-1] + ");"
    print("Create Table Statement:")
    print(CREATE_TABLE_STATEMENT)
    print("---------------------------------------------------------------------------")
    # ---------------------------------------------------
    # Connect to mysql DB
    db = mysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        port=MYSQL_PORT,
        allow_local_infile=True,
    )
    # ---------------------------------------------------
    cursor = db.cursor()
    # Creating table (DDL hence auto commit)
    cursor.execute(CREATE_TABLE_STATEMENT)

    print("table created")
    # --------------------------------------
    # Constructing load statement
    LOAD_STATEMENT = (
        "LOAD DATA LOCAL INFILE '"
        + DATA_FILE
        + "'\nINTO TABLE "
        + MYSQL_DB
        + "."
        + MYSQL_TABLE
        + "\nFIELDS TERMINATED BY '"
        + DELIMITER
        + "'"
        + "\nLINES TERMINATED BY '"
        + NEWLINE
        + "'"
        + "\nIGNORE 1 LINES\n("
    )
    for a_field in column_list:
        LOAD_STATEMENT += "`" + a_field.replace("\n", "") + "`" + ","
    LOAD_STATEMENT = LOAD_STATEMENT[:-1] + ");"
    print("Load Table Statement:")
    print(LOAD_STATEMENT)
    print("---------------------------------------------------------------------------")
    # -------------------------------------
    cursor.execute(LOAD_STATEMENT)
    db.commit()
    db.close()
    print("Complete")
