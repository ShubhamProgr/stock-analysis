import pyodbc

conn_str = (
    r"Driver={ODBC Driver 17 for SQL Server};"
    r"Server=DESKTOP-UDR6P21\SQLEXPRESS;"
    r"Database=Market_data;"
    r"UID=sa;"
    r"PWD=a;"
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
insert_query = """
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
cursor.execute(insert_query)
conn.commit()
cursor.close()
conn.close()

print("✅ Prediction_vs_Actual updated without duplicates.")