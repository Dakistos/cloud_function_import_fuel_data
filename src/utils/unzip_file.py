import os
import requests
import zipfile
import logging

def download_and_extract_zip(url, extract_to='data'):
    """
    Télécharge et extrait un fichier ZIP depuis une URL.

    Args:
        url (str): L'URL du fichier ZIP à télécharger.
        extract_to (str): Répertoire où extraire les fichiers (par défaut : 'data').
    """
    try:
        # Crée le répertoire si nécessaire
        os.makedirs(extract_to, exist_ok=True)

        # Télécharge le fichier ZIP
        zip_path = os.path.join(extract_to, 'downloaded_file.zip')
        logging.info(f"Téléchargement depuis {url}...")
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        logging.info(f"Fichier téléchargé avec succès : {zip_path}")

        # Extraction du fichier ZIP
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)

        logging.info(f"Fichiers extraits dans : {extract_to}")

        # Suppression du fichier ZIP après extraction
        os.remove(zip_path)

    except requests.exceptions.RequestException as e:
        logging.error(f"Erreur lors du téléchargement : {e}")
    except zipfile.BadZipFile as e:
        logging.error(f"Erreur lors de l'extraction du fichier ZIP : {e}")
