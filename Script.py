import os
import re
import pyodbc as odbc
import pandas as pd 

#connection to user's ODBC automatically 
def establish_odbc_connection():
    
    data_sources = odbc.dataSources()
    impala_data_source = [i for i, v in data_sources.items() if re.search("Cloudera ODBC Driver for Impala", v) and not re.search('Sample Cloudera Impala DSN', i)][0]

    if impala_data_source:
        print(f"this is your data source name: {impala_data_source}")
        try:
            conn = odbc.connect("DSN=" + impala_data_source, autocommit=True)
            return conn
        except odbc.Error as e:
            print("ODBC Connection error", e)
            return None
    else:
        print("Cloudera data source not found in ODBC data sources")
        return None
    
#filling the empty fields with fixed values
def preprocess_excel_data(filename, direct):
    
    tablename = re.sub(r"\..*", "", filename )
    excel_file = pd.read_excel(os.path.join(direct,filename))
    
    for col in excel_file.columns:
        for i, val in enumerate(excel_file[col].values):
            if pd.isna(val):
                if excel_file[col].dtype == 'object':
                    excel_file[col] = excel_file[col].fillna("NoValue")
                elif isinstance(val, float):
                    excel_file[col] = excel_file[col].fillna(0)
                elif isinstance(val, int):
                    excel_file[col] = excel_file[col].fillna(0)
    
    table_file = {"tablename":tablename, "excel_file":excel_file}
    return table_file

#regex for cleaning the full filename  
def file_handler(filename):
    
    tablename = re.sub(r"\..*", "", filename)
    return tablename
    
#creating the table in impala if it does not exists
def create_impala_table(filename, direct, conn):
    
    c = conn.cursor()
    tr = preprocess_excel_data(filename, direct)
    
    tablename = tr.get("tablename")
    excel_file = tr.get("excel_file")
    
    columns = excel_file.columns
    create_table_sql = f"CREATE TABLE IF NOT EXISTS dbname.{tablename} (file_name STRING, "
    for column in columns:
        data_type = excel_file[column].dtype
        if data_type == 'int64':
            column_type = 'INT'
        elif data_type == 'float64':
            column_type = 'DOUBLE'
        else:
            column_type = 'STRING'
        create_table_sql += f"{column} {column_type}, "
    create_table_sql = create_table_sql.rstrip(", ") + ")"
    c.execute(create_table_sql)

    return f"Table '{tablename} and {create_table_sql}' created successfully"

#Inserting into the table executed on impala
def insert_into_table(tablename, direct, conn):
    
    c = conn.cursor()
    temp = preprocess_excel_data(filename, direct)
    excel_file = temp.get("excel_file")
    columns = excel_file.columns
    
    data_tuple = [tuple([tablename] + list(t)) for t in excel_file.to_numpy()]
  
    query_insert = f"INSERT INTO dbname.{tablename} VALUES ({', '.join(['?'] * (len(columns) + 1))})"
	
    for value in data_tuple:

        c.execute(query_insert, value)
	
    print("executuon is complete") 

    conn.commit()
    conn.close()

#collecting all the functions under the main     
def main():
    
    tablename = file_handler(filename)
    conn = establish_odbc_connection()
    direct = os.getcwd()
    process_excel = preprocess_excel_data(filename, direct)
    excel_file = process_excel.get("excel_file")
    
    if conn:

        create_table = create_impala_table(filename, direct, conn = conn)
        if create_table:
            inserting_to_table = insert_into_table(tablename, direct, conn)
        else:
            print("Execution failed..")

#running the script
if __name__ == "__main__":
    
    filename = str(input("Enter your file name in the current direcotry: "))
#     filename = "test22.xlsx"
    
    main()
