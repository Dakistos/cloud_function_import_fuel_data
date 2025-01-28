import logging
import psycopg2
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

def delete_old_prices(conn):
    try:
        with conn.cursor() as cur:
            delete_query = f"""DELETE FROM fuel_prices WHERE created_at < NOW() - INTERVAL '7 days'"""
            cur.execute(delete_query)
            deleted_count = cur.rowcount
            conn.commit()

            logger.info(f"Deleted {deleted_count} old prices datas older than 7 days")

            return deleted_count
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error deleting old prices: {e}")
        raise e


def import_to_fuel_prices(conn):
    try:
        seven_days_ago = datetime.now() - timedelta(days=7)

        # Fuel list and their corresponding columns
        fuel_types = {
                "GAZOLE": "diesel_price",
                "SP95": "sp95_price",
                "SP98": "sp98_price",
                "GPLC": "gplc_price",
                "E85": "e85_price",
                "E10": "e10_price",
        }

        # For each fuel type, import the data
        for fuel_code, price_column in fuel_types.items():
            # Query to insert data of less than 7 days
            insert_query = f"""
                INSERT INTO fuel_prices (station_id, fuel_type_id, price, created_at, updated_at)
                SELECT 
                    station_id,
                    (SELECT id FROM fuel_types WHERE code = %s),
                    {price_column},
                    imported_date,
                    {price_column}_updated_at
                FROM instant_fr_fuel_price
                WHERE imported_date >= %s
                AND {price_column} IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM fuel_prices fp
                    WHERE fp.station_id = instant_fr_fuel_price.station_id
                    AND fp.fuel_type_id = (SELECT id FROM fuel_types WHERE code = %s)
                    AND fp.created_at = instant_fr_fuel_price.imported_date
                );
                """
            conn.cursor().execute(insert_query, (fuel_code, seven_days_ago, fuel_code))
            conn.commit()
            logger.info(f"Data for {fuel_code} imported successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error importing data: {e}")
        raise e
