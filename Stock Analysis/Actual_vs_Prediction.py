from dotenv import load_dotenv
import os
import pyodbc

load_dotenv()

MSSQL_SERVER = os.getenv("MSSQL_SERVER")
MSSQL_DATABASE = os.getenv("MSSQL_DATABASE")
MSSQL_USERNAME = os.getenv("MSSQL_USERNAME")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD")
MSSQL_DRIVER = os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server")

CREATE_TABLE_QUERY = """
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Prediction_vs_Actual' AND xtype='U')
CREATE TABLE Prediction_vs_Actual (
    [Company] NVARCHAR(255),
    [Ticker] NVARCHAR(50),
    [Date] DATE,
    [Predicted_Closing_Price] FLOAT,
    [Actual_Closing_Price] FLOAT
)
"""

INSERT_QUERY = """
INSERT INTO Prediction_vs_Actual (
    [Company], 
    [Ticker], 
    [Date], 
    [Predicted_Closing_Price], 
    [Actual_Closing_Price]
)
SELECT 
    Final_Analysis.[Company],
    Final_Analysis.[Ticker],
    Final_Analysis.[Prediction_Date],
    Final_Analysis.[Predicted_Closing_Price],
    StockData.[Close]
FROM Final_Analysis 
JOIN StockData 
    ON Final_Analysis.[Ticker] = StockData.[Ticker] 
    AND Final_Analysis.[Prediction_Date] = StockData.[Date]
WHERE NOT EXISTS (
    SELECT 1 
    FROM Prediction_vs_Actual 
    WHERE Prediction_vs_Actual.[Ticker] = StockData.[Ticker] 
      AND Prediction_vs_Actual.[Date] = Final_Analysis.[Prediction_Date]
)
"""
conn_str = (
    f"DRIVER={{{MSSQL_DRIVER}}};"
    f"SERVER={MSSQL_SERVER};"
    f"DATABASE={MSSQL_DATABASE};"
    f"UID={MSSQL_USERNAME};"
    f"PWD={MSSQL_PASSWORD};"
)

conn = None
cursor = None
try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    cursor.execute(CREATE_TABLE_QUERY)
    conn.commit()
    print("Table 'Prediction_vs_Actual' checked/created.")

    cursor.execute(INSERT_QUERY)
    conn.commit()
    print("Prediction_vs_Actual updated without duplicates.")
except Exception as e:
    print("Failed to update Prediction_vs_Actual:", e)
    raise
finally:
    if cursor:
        try:
            cursor.close()
        except:
            pass
    if conn:
        try:
            conn.close()
        except:
            pass