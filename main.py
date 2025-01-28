import logging
from datetime import datetime, timezone
import os
from flask import jsonify
from src.database.connection import create_connection
from src.data_processing.csv_processor import process_csv
from src.utils.download_csv_file import download_csv_file
from src.data_processing.update_prices import delete_old_prices, import_to_fuel_prices

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def fuel_import(request):
    """Entry point for the serveless function"""

    try:
        logger.info("Starting treatment")

        conn = create_connection()
        if conn is None:
            raise Exception("Fail to connect to the databse")

        try:
            """Downloading and handle CSV"""
            csv_data = download_csv_file()
            rows_processed = process_csv(conn, csv_data)
            
            deleted_count = delete_old_prices(conn)
            imported_count = import_to_fuel_prices(conn)
            
            return jsonify({
                    'message': 'Import successfully',
                    'rows_processed': rows_processed,
                    'prices_imported': imported_count,
                    'old_data_deleted': deleted_count,
                    'timestamp': datetime.now(timezone.utc).isoformat()
            }), 200

        finally:
            conn.close()
            logger.info("Database connection closed")

    except Exception as e:
        logger.error(f"Operation failed: {str(e)}")
        return jsonify({'error': str(e)}), 500
