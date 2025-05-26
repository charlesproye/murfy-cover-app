import os
from datetime import datetime
from core.s3_utils import S3Service

def generate_presigned_url_and_upload(local_file_path, custom_filename=None):
    """
    Génère une URL pré-signée et upload un fichier avec un nom personnalisé
    
    Args:
        local_file_path (str): Chemin du fichier local à uploader
        custom_filename (str, optional): Nom personnalisé pour le fichier distant
    """
    s3 = S3Service()
    bucket_name = os.getenv("S3_BUCKET")
    folder_name = 'response/ituran/'
    
    # Générer le nom du fichier
    if custom_filename is None:
        current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
        remote_filename = f"ituran_data_{current_date}.csv"
    else:
        remote_filename = custom_filename
    
    # Générer l'URL pré-signée
    response = s3.generate_presigned_post(
        key=f'{folder_name}{remote_filename}',
        expires_in=600000
    )
    
    # Upload le fichier
    try:
        with open(local_file_path, 'rb') as file_obj:
            files = {
                'file': (remote_filename, file_obj, 'application/octet-stream')
            }
            
            response_upload = requests.post(
                response['url'],
                data=response['fields'],
                files=files
            )
            
            if response_upload.status_code == 204:
                print(f"Upload successful! File uploaded as: {remote_filename}")
                return True
            else:
                print("Upload failed:", response_upload.text)
                return False
                
    except Exception as e:
        print(f"Error during upload: {str(e)}")
        return False

# Utilisation
local_file = "src/ingestion/ituran/ho.csv"
custom_name = "ho.csv"

# Upload avec un nom personnalisé
generate_presigned_url_and_upload(local_file, custom_name)

# Ou upload avec un nom généré automatiquement
# generate_presigned_url_and_upload(local_file)
