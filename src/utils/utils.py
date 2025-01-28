from datetime import datetime, timezone

def convert_to_boolean(value):
    return str(value).lower().strip() == 'oui'


def parse_datetime(value):
    if not value or value.strip() == '':
        return None
    try:
        return datetime.strptime(value.strip(), '%Y-%m-%dT%H:%M:%S%z')
    except ValueError:
        print(f"Avertissement: Impossible de parser la date/heure: {value}")
        return None


def string_to_pg_array(value):
    if not value or value.strip() == '':
        return '{}'
    # Divide string elements, delete useless spaces, and surrounded each elements with quote
    elements = [f'"{elem.strip()}"' for elem in value.split(',') if elem.strip()]
    # Retourne le format de tableau PostgreSQL
    return '{' + ','.join(elements) + '}'