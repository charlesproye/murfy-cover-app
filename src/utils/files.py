"""
This module provides convenient function to load and save from the
application's `data/` folder (as defined in the config).
"""
import gzip
import io
import logging
import os
import tarfile
import boto3
from abc import ABC, abstractmethod

import pandas as pd
import s3fs
from bib_models_data_ev.core.config import settings


class ABCFileManager(ABC):
    @abstractmethod
    def get_filepath(self, relative_path, create_folders=False) -> str:
        raise NotImplementedError()

    @abstractmethod
    def open(self, relative_path, *args, **kwarg):
        raise NotImplementedError()

    def ensure_folder_exists(s3, thing: pd.DataFrame, path, *args, **kwargs):
        s3_path = path if isinstance(path, str) and path.startswith('s3://') else 's3://' + path
        folder_path = os.path.dirname(s3_path)
        print("folder_path:", folder_path)
        print("s3_path:", s3_path)

        csv_data = thing.to_csv(index=False)
        file_name = os.path.basename(s3_path)
        full_path = f"{folder_path}/{file_name}"
        session = boto3.Session(
            aws_access_key_id='SCW68HH2T2FRENB5T5JT',  # S3_KEY de Scaleway
            aws_secret_access_key='9d33d136-18fe-409d-8927-bf3ff8023faf',  # S3_SECRET de Scaleway
            region_name='fr-par'  # Région Scaleway
        )

        s3 = session.client(
            's3',
            endpoint_url='https://s3.fr-par.scw.cloud'  # Point de terminaison Scaleway
        )

        bucket_name = 'bib-platform-dev-data'

        s3_path_parts = full_path.split('raw_ts')
        if len(s3_path_parts) > 1:
            relative_path = 'raw_ts' + s3_path_parts[1]
            print(relative_path)
        else:
            print("Path does not contain 'raw_ts':", full_path)

        
        try:
            if not fs.exists(folder_path):
                print("Folder does not exist, creating it.")
                fs.makedirs(folder_path)
            
            response = s3.put_object(Bucket=bucket_name, Key=relative_path, Body=csv_data)
            print("File written successfully", response)
        except Exception as e:
            print(f"Error in ensure_folder_exists: {str(e)}")
            print(f"Error type: {type(e)}")
            print(f"fs type: {type(fs)}")
            print(f"thing type: {type(thing)}")
            print(f"path: {path}")
            print(f"s3_path: {s3_path}")
            print(f"folder_path: {folder_path}")
            print(f"full_path: {full_path}")
            
        


    def save(self, fs: s3fs.S3FileSystem, thing: pd.DataFrame, relative_path, *args, **kwargs):
        """Save object (usually dataframe) to file

        Additional ``*args`` and ``**kwargs`` are passed to the underlying function.


        >>> df = pd.DataFrame()
        >>> files.save(df, 'clean/iris.pickle')
        """
        path = self.get_filepath(relative_path)
        print("path save", path)
        self.ensure_folder_exists(fs, thing, path)  # Ensure the folder exists
        basename = os.path.basename(path)
        if ".pickle" in basename:
            # assuming this is a pandas df
            logging.debug("Saving object to " + path)
            thing.to_pickle(path, *args, **kwargs)
        elif ".csv" in basename:
            # assuming this is a pandas df
            logging.debug("Saving object to " + path)
            print("après debug")
            print("thing", thing)
            print("type(thing)", type(thing))
            thing.to_csv(path, *args, **kwargs)
        elif ".json" in basename:
            # assuming this is a pandas df
            # logging.debug("Saving object to " + path)
            thing.to_json(path, *args, **kwargs)
        elif ".xml" in basename:
            # assuming this is a pandas df
            logging.debug("Saving object to " + path)
            thing.to_xml(path, *args, **kwargs)
        else:
            logging.error("Cannot save to {}. Unkown extension".format(path))
            raise NotImplementedError("Extension of {} not handled by files.save()".format(path))
        response = s3.put_object(Bucket=bucket_name, Key=relative_path, Body=csv_data)

    def load(self, relative_path, *args, **kwargs) -> pd.DataFrame:
        """Loads .pickle and .csv from file relative to  ``data/`` folder

        Parameters
        ----------
        relative_path: str
            path relative to the application ``data/`` folder
        args:
            those are passed to the inner load function
        kwargs:
            those are passed to the inner load function


        >>> # this loads the file /path/to/data/raw/iris.csv
        >>> df = files.load('raw/iris.csv', nrows=10, dtype='str')
        """
        path = self.get_filepath(relative_path)
        basename = os.path.basename(path)
        print("path load", path)
        logging.debug("Loading from: " + path)
        if ".pickle" in basename:
            return pd.read_pickle(path, *args, **kwargs)
        elif ".csv" in basename:
            return pd.read_csv(path, dtype={'36': object, '38': object}, *args, **kwargs, low_memory = False)
        elif ".json" in basename:
            return pd.read_json(path, *args, **kwargs)
        elif ".xml" in basename:
            return pd.read_xml(path, *args, **kwargs)
        elif ".xlsx" in basename:
            return pd.read_excel(path, *args, **kwargs)
        else:
            logging.error("Impossible to load from {}. Unkown extention".format(path))
            raise NotImplementedError("Extension of {} not handled by files.load()".format(path))

    def read(self, relative_path):
        mode = "r"
        path = self.get_filepath(relative_path)
        basename = os.path.basename(path)

        def open_single_tar_gz():
            with self.open(path, f"{mode}b") as f:
                logging.debug("unzip " + basename)
                with gzip.GzipFile(fileobj=f) as g:
                    logging.debug("untar " + basename)
                    with tarfile.open(fileobj=io.BytesIO(g.read()), mode=f"{mode}") as tar:
                        logging.debug("open " + basename)
                        return tar.extractfile(tar.getnames()[0])

        if basename.endswith(".csv.tar.gz"):
            return open_single_tar_gz()

        if basename.endswith(".xml.tar.gz"):
            return open_single_tar_gz()

        if basename.endswith(".json.tar.gz"):
            return open_single_tar_gz()

        if basename.endswith(".pickle.tar.gz"):
            return open_single_tar_gz()

        if basename.endswith(".tar.gz"):
            with self.open(path, f"{mode}b") as f:
                with gzip.GzipFile(fileobj=f) as g:
                    return tarfile.open(fileobj=io.BytesIO(g.read()), mode=f"{mode}")

        if basename.endswith(".gz"):
            with self.open(path, f"{mode}b") as f:
                return gzip.GzipFile(fileobj=f)

        return open(path, mode)

##############For S3FS 

# class ABCFileManager(ABC):
#     @abstractmethod
#     def get_filepath(self, relative_path, create_folders=False) -> str:
#         raise NotImplementedError()

#     @abstractmethod
#     def open(self, relative_path, *args, **kwarg):
#         raise NotImplementedError()

#     def ensure_folder_exists(self, fs, thing: pd.DataFrame, path, *args, **kwargs):
#         s3_path = path if isinstance(path, str) and path.startswith('s3://') else 's3://' + path
#         folder_path = os.path.dirname(s3_path)
#         print("folder_path:", folder_path)
#         print("s3_path:", s3_path)

#         csv_data = thing.to_csv(index=False)
#         file_name = os.path.basename(s3_path)
#         full_path = f"{folder_path}/{file_name}"
#         session = boto3.Session(
#             aws_access_key_id='SCW68HH2T2FRENB5T5JT',  # S3_KEY de Scaleway
#             aws_secret_access_key='9d33d136-18fe-409d-8927-bf3ff8023faf',  # S3_SECRET de Scaleway
#             region_name='fr-par'  # Région Scaleway
#         )

#         s3 = session.client(
#             's3',
#             endpoint_url='https://s3.fr-par.scw.cloud'  # Point de terminaison Scaleway
#         )

#         bucket_name = 'bib-platform-dev-data'

#         s3_path_parts = full_path.split('raw_ts')
#         if len(s3_path_parts) > 1:
#             relative_path = 'raw_ts' + s3_path_parts[1]
#             print(relative_path)
#         else:
#             print("Path does not contain 'raw_ts':", full_path)

        
#         try:
#             if not fs.exists(folder_path):
#                 print("Folder does not exist, creating it.")
#                 fs.makedirs(folder_path)
            
#             response = s3.put_object(Bucket=bucket_name, Key=relative_path, Body=csv_data)
#             print("File written successfully", response)
#         except Exception as e:
#             print(f"Error in ensure_folder_exists: {str(e)}")
#             print(f"Error type: {type(e)}")
#             print(f"fs type: {type(fs)}")
#             print(f"thing type: {type(thing)}")
#             print(f"path: {path}")
#             print(f"s3_path: {s3_path}")
#             print(f"folder_path: {folder_path}")
#             print(f"full_path: {full_path}")
            
        


#     def save(self, fs: s3fs.S3FileSystem, thing: pd.DataFrame, relative_path, *args, **kwargs):
#         """Save object (usually dataframe) to file

#         Additional ``*args`` and ``**kwargs`` are passed to the underlying function.


#         >>> df = pd.DataFrame()
#         >>> files.save(df, 'clean/iris.pickle')
#         """
#         path = self.get_filepath(relative_path)
#         print("path save", path)
#         self.ensure_folder_exists(fs, thing, path)  # Ensure the folder exists
#         basename = os.path.basename(path)
#         if ".pickle" in basename:
#             # assuming this is a pandas df
#             logging.debug("Saving object to " + path)
#             thing.to_pickle(path, *args, **kwargs)
#         elif ".csv" in basename:
#             # assuming this is a pandas df
#             logging.debug("Saving object to " + path)
#             print("après debug")
#             print("thing", thing)
#             print("type(thing)", type(thing))
#             thing.to_csv(path, *args, **kwargs)
#         elif ".json" in basename:
#             # assuming this is a pandas df
#             # logging.debug("Saving object to " + path)
#             thing.to_json(path, *args, **kwargs)
#         elif ".xml" in basename:
#             # assuming this is a pandas df
#             logging.debug("Saving object to " + path)
#             thing.to_xml(path, *args, **kwargs)
#         else:
#             logging.error("Cannot save to {}. Unkown extension".format(path))
#             raise NotImplementedError("Extension of {} not handled by files.save()".format(path))
#         response = s3.put_object(Bucket=bucket_name, Key=relative_path, Body=csv_data)

#     def load(self, relative_path, *args, **kwargs) -> pd.DataFrame:
#         """Loads .pickle and .csv from file relative to  ``data/`` folder

#         Parameters
#         ----------
#         relative_path: str
#             path relative to the application ``data/`` folder
#         args:
#             those are passed to the inner load function
#         kwargs:
#             those are passed to the inner load function


#         >>> # this loads the file /path/to/data/raw/iris.csv
#         >>> df = files.load('raw/iris.csv', nrows=10, dtype='str')
#         """
#         path = self.get_filepath(relative_path)
#         basename = os.path.basename(path)
#         print("path load", path)
#         logging.debug("Loading from: " + path)
#         if ".pickle" in basename:
#             return pd.read_pickle(path, *args, **kwargs)
#         elif ".csv" in basename:
#             return pd.read_csv(path, dtype={'36': object, '38': object}, *args, **kwargs, low_memory = False)
#         elif ".json" in basename:
#             return pd.read_json(path, *args, **kwargs)
#         elif ".xml" in basename:
#             return pd.read_xml(path, *args, **kwargs)
#         elif ".xlsx" in basename:
#             return pd.read_excel(path, *args, **kwargs)
#         else:
#             logging.error("Impossible to load from {}. Unkown extention".format(path))
#             raise NotImplementedError("Extension of {} not handled by files.load()".format(path))

#     def read(self, relative_path):
#         mode = "r"
#         path = self.get_filepath(relative_path)
#         basename = os.path.basename(path)

#         def open_single_tar_gz():
#             with self.open(path, f"{mode}b") as f:
#                 logging.debug("unzip " + basename)
#                 with gzip.GzipFile(fileobj=f) as g:
#                     logging.debug("untar " + basename)
#                     with tarfile.open(fileobj=io.BytesIO(g.read()), mode=f"{mode}") as tar:
#                         logging.debug("open " + basename)
#                         return tar.extractfile(tar.getnames()[0])

#         if basename.endswith(".csv.tar.gz"):
#             return open_single_tar_gz()

#         if basename.endswith(".xml.tar.gz"):
#             return open_single_tar_gz()

#         if basename.endswith(".json.tar.gz"):
#             return open_single_tar_gz()

#         if basename.endswith(".pickle.tar.gz"):
#             return open_single_tar_gz()

#         if basename.endswith(".tar.gz"):
#             with self.open(path, f"{mode}b") as f:
#                 with gzip.GzipFile(fileobj=f) as g:
#                     return tarfile.open(fileobj=io.BytesIO(g.read()), mode=f"{mode}")

#         if basename.endswith(".gz"):
#             with self.open(path, f"{mode}b") as f:
#                 return gzip.GzipFile(fileobj=f)

#         return open(path, mode)


# class FileManager(ABCFileManager):
#     """Utility class to handle data files contained in a root folder

#     Parameters
#     ----------
#     root: str
#         absolute path to the root data folder


#     >>> files = FileManager('/path/to/data/')
#     >>> df = files.load('raw/iris.csv')
#     # Loads dataframe from /path/to/data/raw/iris.csv
#     >>> files.save(df, 'clean/iris.pickle')
#     # saves to /path/to/data/clean/iris.pickle
#     # end ensures 'clean' folder exists
#     """

#     def __init__(self, root: str = ""):
#         root = os.path.abspath(root)
#         if not os.path.exists(root):
#             raise ValueError("Provided path {} does not exist.".format(root))
#         self.root = root

#     def get_filepath(self, relative_path, create_folders=False) -> str:
#         path = os.path.abspath(os.path.join(self.root, relative_path))
#         if create_folders:
#             self.ensure_folder_exists(path)
#         return path

#     def ensure_folder_exists(self, abspath):
#         """creates subfolders if necessary"""
#         if not os.path.isabs(abspath):
#             raise ValueError('path "{}" is not absolute'.format(abspath))

#         path, extension = os.path.splitext(abspath)
#         if extension:
#             # if it was a file we only take the folder
#             path = os.path.dirname(path)
#         if not os.path.exists(path):
#             os.makedirs(path)

#     def open(self, relative_path, *args, **kwarg):
#         path = self.get_filepath(relative_path)
#         return open(path, *args, **kwarg)


# class S3FileManager(ABCFileManager):
#     def __init__(self, key: str, secret: str, client_kwargs: dict, bucket: str, root: str = ""):
#         self.bucket = bucket
#         self.storage_options = dict(anon=False, key=key, secret=secret, client_kwargs=client_kwargs)
#         self.fs = s3fs.S3FileSystem(False, key, secret, client_kwargs=client_kwargs)
#         if root.startswith("/"):
#             root = root[1:]
#         self.root = os.path.join(bucket, root)

#     @classmethod
#     def from_fs(cls, fs: s3fs.S3FileSystem, bucket: str, root: str = "") -> "S3FileManager":
#         return cls(fs.key, fs.secret, fs.client_kwargs, bucket, root)

#     def ls_files(fs: s3fs.S3FileSystem, bucket: str, key: str, secret: str, client_kwargs: dict, root: str = ""):
#         storage_options = dict(anon=False, key=key, secret=secret, client_kwargs=client_kwargs)
#         fs2 = s3fs.S3FileSystem(False, key, secret, client_kwargs=client_kwargs)
#         files = fs2.ls(bucket)
#         return files

#     def get_filepath(self, relative_path) -> str:
#         return f"s3://{self.get_fullpath(relative_path)}"

#     def get_fullpath(self, relative_path) -> str:
#         path = os.path.join(self.root, relative_path)
#         return path

#     def open(self, relative_path, *args, **kwarg):
#         path = self.get_fullpath(relative_path)
#         return self.fs.open(path, *args, **kwarg)

#     def save(self, fs: s3fs.S3FileSystem, thing: pd.DataFrame, relative_path, *args, **kwargs):
#         kwargs["storage_options"] = self.storage_options
#         return super().save(fs, thing, relative_path, *args, **kwargs)

#     def load(self, relative_path, *args, **kwargs) -> pd.DataFrame:
#         kwargs["storage_options"] = self.storage_options
#         return super().load(relative_path, *args, **kwargs)

