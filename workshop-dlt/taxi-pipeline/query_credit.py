import duckdb

conn = duckdb.connect('taxi_pipeline.duckdb', read_only=True)

# Get distinct payment types
payment_types = conn.execute('SELECT DISTINCT Payment_Type FROM taxi_pipeline_dataset._data_engineering_zoomcamp_api ORDER BY Payment_Type').fetchall()
print('Payment Types:')
for pt in payment_types:
    print(f'  - {repr(pt[0])}')

# Query to calculate the proportion of credit card payments
result = conn.execute('''
    SELECT 
        COUNT(CASE WHEN Payment_Type = 'Credit' THEN 1 END) as credit_trips,
        COUNT(*) as total_trips
    FROM taxi_pipeline_dataset._data_engineering_zoomcamp_api
''').fetchall()

for row in result:
    credit_trips = row[0]
    total_trips = row[1]
    proportion = round(100.0 * credit_trips / total_trips, 2)
    print(f'\nCredit Card Trips: {credit_trips}')
    print(f'Total Trips: {total_trips}')
    print(f'Proportion of Credit Card Payments: {proportion}%')

conn.close()
