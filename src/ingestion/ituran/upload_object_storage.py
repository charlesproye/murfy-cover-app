import requests
import os

def upload_with_presigned_url(local_file_path: str, presigned_url: str, presigned_fields: dict, remote_filename: str = None):
    """
    Upload un fichier en utilisant une URL pré-signée et ses champs
    
    Args:
        local_file_path (str): Chemin du fichier local à uploader
        presigned_url (str): URL pré-signée S3
        presigned_fields (dict): Champs pré-signés requis pour l'upload
        remote_filename (str, optional): Nom du fichier distant. Si None, utilise le nom du fichier local
    """
    # Si pas de nom distant spécifié, utiliser le nom local
    if remote_filename is None:
        remote_filename = os.path.basename(local_file_path)

    try:
        # Créer le multipart form
        files = {}
        
        # Ajouter d'abord tous les champs pré-signés
        for key, value in presigned_fields.items():
            files[key] = (None, value)
        
        # Ajouter le Content-Type
        files['Content-Type'] = (None, 'text/csv')
            
        # Ajouter le fichier
        files['file'] = (remote_filename, open(local_file_path, 'rb'), 'text/csv')

        # Upload du fichier
        response = requests.post(
            presigned_url,
            files=files
        )

        if response.status_code == 204:
            print(f"Upload successful! File uploaded as: {remote_filename}")
            return True
        else:
            print("Upload failed:", response.text)
            print("Response status code:", response.status_code)
            print("Fields used:", files)
            return False
            
    except Exception as e:
        print(f"Error during upload: {str(e)}")
        return False
    finally:
        # Fermer le fichier
        if 'file' in files:
            files['file'][1].close()

# Exemple d'utilisation
if __name__ == "__main__":
    # URL et champs pré-signés
    presigned_url = "https://s3.fr-par.scw.cloud/bib-platform-prod-data"
    presigned_fields = {
        "key": "response/ituran/${filename}",
        "acl": "private",
        "x-amz-algorithm": "AWS4-HMAC-SHA256",
        "x-amz-credential": "SCW9P6Q1T26F2JGSC1AS/20241217/fr-par/s3/aws4_request",
        "x-amz-date": "20241217T082022Z",
        "policy": "eyJleHBpcmF0aW9uIjogIjIwMjQtMTItMjRUMDc6MDA6MjJaIiwgImNvbmRpdGlvbnMiOiBbWyJzdGFydHMtd2l0aCIsICIka2V5IiwgInJlc3BvbnNlL2l0dXJhbi8iXSwgWyJzdGFydHMtd2l0aCIsICIkQ29udGVudC1UeXBlIiwgIiJdLCB7ImFjbCI6ICJwcml2YXRlIn0sIFsiY29udGVudC1sZW5ndGgtcmFuZ2UiLCAwLCAxMDQ4NTc2MF0sIHsiYnVja2V0IjogImJpYi1wbGF0Zm9ybS1wcm9kLWRhdGEifSwgWyJzdGFydHMtd2l0aCIsICIka2V5IiwgInJlc3BvbnNlL2l0dXJhbi8iXSwgeyJ4LWFtei1hbGdvcml0aG0iOiAiQVdTNC1ITUFDLVNIQTI1NiJ9LCB7IngtYW16LWNyZWRlbnRpYWwiOiAiU0NXOVA2UTFUMjZGMkpHU0MxQVMvMjAyNDEyMTcvZnItcGFyL3MzL2F3czRfcmVxdWVzdCJ9LCB7IngtYW16LWRhdGUiOiAiMjAyNDEyMTdUMDgyMDIyWiJ9XX0=",
        "x-amz-signature": "ad7ccd0ff3008e3a7d58de69ac6a950a05f169efd3c81518162f92d2f74c2995"
    }

    # Fichier à uploader
    files_to_upload = [
        {
            'local_path': 'src/ingestion/ituran/ho.csv',
            'remote_name': 'data_file1.csv'
        },
        {
            'local_path': 'src/ingestion/ituran/ho.csv',
            'remote_name': 'data_file2.csv'
        }
    ]

    # Upload du fichier
    for file_info in files_to_upload:
        upload_with_presigned_url(
            file_info['local_path'],
            presigned_url,
            presigned_fields,
            file_info['remote_name']
        )
