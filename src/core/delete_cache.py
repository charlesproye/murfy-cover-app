from .s3_utils import S3Service
import logging

def main():
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    # Initialize S3 bucket connection
    s3 = S3Service()
    
    # Define the folder to delete
    cache_folder = "raw_tss/tesla/parsed_responses_cache/"
    
    logger.info(f"Starting to delete {cache_folder}")
    
    # List all subfolders to help debug
    try:
        root_folders = s3.list_subfolders("")
        logger.info(f"Root folders in bucket: {root_folders}")
        
        if 'raw_tss' in root_folders:
            tesla_folders = s3.list_subfolders("raw_tss/tesla")
            logger.info(f"Folders in raw_tss/tesla: {tesla_folders}")
    except Exception as e:
        logger.error(f"Error listing folders: {str(e)}")
    
    # First, verify the folder exists and list its contents
    keys = s3.list_keys(cache_folder)
    logger.info(f"Found {len(keys)} objects in the cache folder")
    
    if len(keys) == 0:
        logger.warning("No files found in the specified folder. Check if the path is correct.")
        return
        
    logger.info("First few files found:")
    for key in keys[:5]:
        logger.info(f"- {key}")
    
    try:
        s3.delete_folder(cache_folder)
        logger.info("Cache deletion completed successfully!")
    except Exception as e:
        logger.error(f"Error while deleting cache: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()
