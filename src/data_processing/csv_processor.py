import csv, logging
from datetime import datetime, timezone
from psycopg2.extras import execute_batch
from .column_mapping import column_mapping
from src.utils.utils import convert_to_boolean, parse_datetime, string_to_pg_array

# logging config
logger = logging.getLogger(__name__)


def process_csv(conn, csv_data, batch_size=1000):
    array_columns = ['available_fuel', 'unavailable_fuel', 'temporary_out_of_stock_fuel',
                     'definitive_out_of_stock_fuel', 'provided_services', 'detailed_schedules']

    with conn.cursor() as cur:
        reader = csv.DictReader(csv_data, delimiter=";")

        # Verify and clean csv header
        if '\ufeffid' in reader.fieldnames:
            reader.fieldnames[reader.fieldnames.index('\ufeffid')] = 'id'

        batch = []
        rows_processed = 0

        # Create DB column with the column_mapping
        db_columns = list(column_mapping.values()) + ['geom_coordinates', 'imported_date']

        # Build dynamically the SQL query
        columns_str = ', '.join(db_columns)
        placeholders = ', '.join(['%s' for _ in db_columns])
        insert_query = f"""
                INSERT INTO instant_fr_fuel_price ({columns_str})
                VALUES ({placeholders})
            """

        for row_num, row in enumerate(reader, start=1):
            # Create a new dictionnary with the DB columns name
            db_row = {}
            for csv_col, db_col in column_mapping.items():
                if csv_col in row:
                    value = row[csv_col]
                    if csv_col == 'Automate 24-24 (oui/non)':
                        value = convert_to_boolean(value)
                    elif 'mis à jour le' in csv_col or 'Début rupture' in csv_col:
                        value = parse_datetime(value)
                    elif db_col in array_columns:
                        value = string_to_pg_array(value)
                    elif value == '':
                        value = None
                    db_row[db_col] = value

                lat, lon = map(float, row['geom'].split(','))
                db_row['geom_coordinates'] = f"POINT({lon} {lat})"

                db_row['imported_date'] = datetime.now(timezone.utc)

            # Add rows to batch in the column order
            batch.append(tuple(db_row.get(col) for col in db_columns))

            rows_processed += 1

            if len(batch) >= batch_size:
                execute_batch(cur, insert_query, batch)
                conn.commit()
                batch = []

        # Insert last rows
        if batch:
            execute_batch(cur, insert_query, batch)
            conn.commit()

        return rows_processed
