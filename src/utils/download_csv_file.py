import requests
import os
import logging
from io import StringIO

logger = logging.getLogger(__name__)

def download_csv_file():

    url = os.environ['CSV_URL']

    try:
        response = requests.get(url)
        response.raise_for_status()
        return StringIO(response.text)
    except KeyError:
        logger.error("Environment variable CSV_URL is not defined")
    except Exception as e:
        logger.error(f"Fail to download CSV file: {str(e)}")
        raise
