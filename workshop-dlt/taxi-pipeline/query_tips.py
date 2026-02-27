import duckdb

conn = duckdb.connect('taxi_pipeline.duckdb', read_only=True)

result = conn.execute('''
    SELECT SUM(Tip_Amt) as total_tips
    FROM taxi_pipeline_dataset._data_engineering_zoomcamp_api
''').fetchall()

print('Total tips:', result[0][0])

conn.close()
