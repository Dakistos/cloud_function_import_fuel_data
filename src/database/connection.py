import logging
import os

import psycopg2
from configparser import ConfigParser

logger = logging.getLogger(__name__)

def create_connection():
  try:
    conn = psycopg2.connect(
      host=os.environ['DB_HOST'],
      port=os.environ['DB_PORT'],
      database=os.environ['DB_NAME'],
      user=os.environ['DB_USER'],
      password=os.environ['DB_PASSWORD']
    )
    return conn
  except Exception as e:
    logger.error(f"Fail to connect to the database: {str(e)}")
    return None