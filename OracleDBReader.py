import os
import cx_Oracle
import pandas as pd


class OracleDBReader:
    def __init__(self, ip, port, sid, userName, password):
        self.ip = ip
        self.port = port
        self.sid = sid
        self.userName = userName
        self.password = password

    def readDB(self, sql):
        # Set Oracle client library location in the system PATH
        location = r'.\\instantclient_21_7'
        os.environ["PATH"] = location + ";" + os.environ["PATH"]

        # Create DSN for Oracle connection
        dsn = cx_Oracle.makedsn(self.ip, self.port, self.sid)

        # Connect to Oracle database
        db = cx_Oracle.connect(self.userName, self.password, dsn)
        cursor = db.cursor()

        # Execute SQL query
        cursor.execute(sql)
        out_data = cursor.fetchall()

        # Convert query result to a DataFrame
        df_oracle = pd.DataFrame(out_data)

        # Set column names
        col_names = [row[0] for row in cursor.description]
        df_oracle.columns = col_names

        return df_oracle