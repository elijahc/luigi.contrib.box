import logging
import ntpath
import json
import os
import random
import tempfile
import time
import sys
from redis import Redis
from functools import wraps
from contextlib import contextmanager

import luigi.format
from luigi.target import FileSystem, FileSystemTarget, AtomicLocalFile

from .auth import jwt, DEFAULT_CONFIG
from .utils import accept_trailing_slash_in_existing_dirpaths, accept_trailing_slash, is_redis_available

if sys.version_info[0] < 3: 
    from StringIO import StringIO
else:
    from io import StringIO,BytesIO

# TODO: Rename all instances of DEFAULT_CONFIG_FP to DEFAULT_CONFIG
DEFAULT_CONFIG_FP = DEFAULT_CONFIG

logger = logging.getLogger('luigi-interface')
import boxsdk
from boxsdk.auth import JWTAuth, RedisManagedJWTAuth
from boxsdk import Client

# DEFAULT_ROOT=os.path.expanduser(os.path.join('~','.emu'))
DEFAULT_CONFIG=os.path.expanduser('~/.emu/config.json')

def jwt_auth():
    auth = JWTAuth.from_settings_file(os.path.expanduser(DEFAULT_CONFIG))
    if is_redis_available(Redis()):
        from boxsdk.auth import RedisManagedJWTAuth
        auth = RedisManagedJWTAuth.from_settings_file(DEFAULT_CONFIG)
    return auth

def jwt(cred_fp=DEFAULT_CONFIG):
    # Load JWT config file from default location
    print('loading JWTAuth')
    config = JWTAuth.from_settings_file(os.path.expanduser(cred_fp))
    return Client(config)

class BoxClient(FileSystem):
    """Box client for authentication, designed to be used by the :py:class:`BoxTarget` class.

    Parameters
    ----------
    path_to_config : str, optional
        path to a valid JWT json configuration

    user_agent : str
        user agent to identify as

    Attributes
    ----------
    token : same as path_to_config

    Methods
    -------
    file(fid=12345678)
    folder(fid=987654)
    exists(path='/path/to/file')
    isdir()
    mkdir()
    remove()
    download_as_bytes()
    upload()

    Raises
    ------
    ValueError
    """

    def __init__(self, auth, user_agent="Luigi"):
        """
        :param boxsdk.auth.Auth auth: path to Box JWT config.json file
        """

        self.auth = auth
        self.conn = Client(self.auth)

    @classmethod
    def from_settings_dictionary(cls,settings_dictionary, use_redis=False, **kwargs):
        if 'boxAppSettings' not in settings_dictionary:
            raise ValueError('boxAppSettings not present in configuration')

        if use_redis:
            auth = RedisManagedJWTAuth.from_settings_dictionary(settings_dictionary,**kwargs)
        else:
            auth = JWTAuth.from_settings_dictionary(settings_dictionary,**kwargs)

        return cls(auth=auth)

    @classmethod
    def from_settings_file(cls, settings_file_sys_path, use_redis=False, **kwargs):
        with open(settings_file_sys_path) as config_file:
            config_dict = json.load(config_file)
            return cls.from_settings_dictionary(config_dict, use_redis=use_redis, **kwargs)

    def file(self, fid):
        return self.conn.file(fid)

    def folder(self,fid):
        return self.conn.folder(fid)

    def exists(self, path):
        try:
            f = path_to_obj(self.conn,path)
            if f.type in ['file','folder']:
                return True
        except ValueError as e:
            return False
        # Implement this

    def search(self,query, limit=100, order='relevance'):
        return self.conn.search().query(query=query, limit=limit, order=order)

    def remove(self, path, recursive=True, skip_trash=True):
        if not self.exists(path):
            return False
        self.conn.files_delete_v2(path)
        return True

    def upload(self, folder_path, local_path):
        if not os.path.exists(local_path):
            raise ValueError('local path {} does not exist'.format(local_path))

        file_name = os.path.split(local_path)[-1]
        remote_filepath = folder_path+'/'+file_name
        if self.exists(remote_filepath):
            logger.warning('File exists, updating contents')
            file = path_to_obj(self.conn, remote_filepath)
            uploaded_file = file.update_contents(local_path)
        elif not self.isdir(folder_path):
            raise ValueError('Invalid folder path {}'.format(folder_path))
        else:
            folder = path_to_obj(self.conn,folder_path)
            if os.path.getsize(local_path)/10**6 > 200:
                logger.warning('File larger than 200Mb, using chunked uploader')
                uploader = folder.get_chunked_uploader(local_path)
                uploaded_file = uploader.start()
            else:
                uploaded_file = folder.upload(local_path)

        return uploaded_file

    def download_as_bytes(self, fid):
        content = self.conn.file(fid).content()
        if isinstance(content,str):
            content = unicode(content, 'utf-8')
        return content

    # def path_to_fid(self,path):
    #     return path_to_fid(client=self.conn,path=path)
    def get_info(self,path):
        return path_to_obj(client=self.conn,path=path)

    def file_id_to_path(self, file_id):
        parent_dirs = []
        f = self.file(file_id).get()
        obj_to_path(f)
        # parent_path = f.path_collection['entries']
        # parent_path = [folder.name for folder in parent_path[1:]]
        # parent_dirs.extend(parent_path)
        # path = '/'+'/'.join(parent_dirs)+'/'
        return path + f.name

    @accept_trailing_slash_in_existing_dirpaths
    def isdir(self, path):
        if path == '/':
            return True
        try:
            f = path_to_obj(self.conn,path)
            return isinstance(f, boxsdk.object.folder.Folder)
        except ValueError as e:
            raise e

    def mkdir(self, path, parents=True, raise_if_exists=False):
        parent_path, new_dir = os.path.split(path)
        print(parent_path)
        parent_folder = path_to_obj(self.conn, parent_path)
        try:
            f = path_to_obj(self.conn,path)
            if f.type is 'file':
                raise luigi.target.NotADirectory()
            elif raise_if_exists:
                raise luigi.target.FileAlreadyExists()
        except ValueError as e:
            print('Make folder!')

    def _exists_and_is_dir(self, path):
        """Auxiliary method, used by the 'accept_trailing_slash' and 'accept_trailing_slash_in_existing_dirpaths' decorators
        :param path: a Dropbox path that does NOT ends with a '/' (even if it is a directory)
        """
        if path == '/':
            return True
        try:
            md = path_to_obj(self.conn,path).get()
            is_dir = isinstance(md, boxsdk.object.folder.Folder)
            return is_dir
        except boxsdk.exception.BoxAPIException:
            return False

class ReadableBoxFile(object):
    """Represents a file inside the Box.com cloud which will be read

    Parameters
    ----------

    file_id : int
        Box.com file ID of the file to be read (https://app.box.com/file/<file_id>)

    client : object, BoxClient
        BoxClient object (initialized with a valid token)

    Attributes
    ---------
    path : str
        path to file from Box.com directory root (always starting with /)
    download_file_location : str
        A temporary filepath on the local filesystem for writing intermediate files 

    closed : bool
        Flag for if the file has been closed

    Methods
    -------
    read()
        read file directly from box.com into memory

    download_to_tmp()
        writes file to location specified in download_file_location

    close()
        sets closed attribute to True
    """
    def __init__(self, file_id, client):
        self.fid = file_id

        self.client = client
        file = self.client.conn.file(file_id).get()
        self.path = obj_to_path(file)
        self.download_file_location = os.path.join(tempfile.mkdtemp(prefix=str(time.time())),
                                                   ntpath.basename(self.path))
        self.closed = False

    def read(self):
        """read file in cloud into memory

        Returns
        -------

        bytes
            byte content of file
        """
        byte_content =  self.client.download_as_bytes(self.fid)

        return byte_content

    def download_to_tmp(self):
        with open(self.download_file_location, 'w') as tmpfile:
            self.client.file(self.fid).download_to(tmpfile)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    def __del__(self):
        self.close()
        if os.path.exists(self.download_file_location):
            os.remove(self.download_file_location)

    def close(self):
        self.closed = True

    def readable(self):
        return True

    def writable(self):
        return False

    def seekable(self):
        return False

class AtomicWritableBoxFile(AtomicLocalFile):
    def __init__(self, path, client):
        """
        Represents a file that will be created inside the Box cloud

        :param str path: Destination path inside Box cloud
        :param BoxClient client: a BoxClient object (initialized with a valid token, for the desired account)
        """
        super(AtomicWritableBoxFile, self).__init__(path)
        self.path = path
        self.client = client

        dir_path,_ = os.path.split(self.path)
        self.folder = path_to_obj(self.client,dir_path)

    def move_to_final_destination(self):
        """After editing the file locally, this function uploads it to the Box.com cloud
        """
        self.client.upload(self.folder.id, self.path)

class BoxTarget(FileSystemTarget):

    def __init__(self, path, file_id=None, auth=None, format=None, user_agent="Luigi"):
        if auth is None:
            self.client = BoxClient.from_settings_file(DEFAULT_CONFIG,use_redis=True)
        else:
            self.client = BoxClient(auth=auth, user_agent=user_agent)

        self.format = format or luigi.format.get_default_format()

        super(BoxTarget, self).__init__(path)

        if file_id is not None and isinstance(file_id,(int,str)):
            try:
                test_file_path = file_id_to_path(int(file_id),client=self.client)
                if test_file_path == self.path:
                    self._file_id =  int(file_id)
            except:
                self._file_id = None

    @property
    def file_id(self):
        if self._file_id is not None:
            return self._file_id
        else:
            self._file_id = int(self.client.get_info(self.path).id)
            return self._file_id

    @property
    def fs(self):
        return self.client

    def open(self, mode):
        if mode not in ('r', 'w', 'rb'):
            raise ValueError("Unsupported open mode '%s'" % mode)

        if mode == 'r':
            rbf = ReadableBoxFile(file_id=self.file_id,client=self.client)
            return StringIO(str(rbf.read(), 'utf-8'))
        elif mode == 'rb':
            rbf = ReadableBoxFile(file_id=self.file_id,client=self.client)
            return BytesIO(rbf.read())

            # return self.format.pipe_reader(ReadableBoxFile(self.fid, self.client))
            # fp = rbf.download_to_tmp()
            # print('downloading to:\n {}'.format(rbf.download_file_location))
            # return open(rbf.download_file_location, 'r')
        else:
            return self.format.pipe_reader(AtomicWritableBoxFile(self.path, self.client))

    @contextmanager
    def temporary_path(self):
        tmp_dir = tempfile.mkdtemp()
        dst_path = os.path.join(tmp_dir,ntpath.basename(self.path))
        num = random.randrange(0, 1e10)
        temp_path = '{}{}luigi-tmp-{:010}{}'.format(
            tmp_dir, os.sep,
            num, ntpath.basename(self.path))

        yield temp_path
        # We won't reach here if there was an user exception.

        os.rename(temp_path,dst_path)

        uploaded_file = self.fs.upload(ntpath.dirname(self.path),dst_path)

def obj_to_path(obj):
    folders =[f.name for f in obj.path_collection['entries'][1:]]
    path = '/'+'/'.join(folders)+'/'+obj.name
    return path

def path_to_root(obj):
    parent_dirs = []
    parent_path = obj.path_collection['entries']
    parent_path = [folder.name for folder in parent_path[1:]]
    parent_dirs.extend(parent_path)
    path = '/'+'/'.join(parent_dirs)+'/'
    return path + obj.name 

def file_id_to_path(file_id, client=None):
    if client is None:
        client = Client(jwt_auth())
    parent_dirs = []
    f = client.file(file_id).get()
    parent_path = f.path_collection['entries']
    parent_path = [folder.name for folder in parent_path[1:]]
    parent_dirs.extend(parent_path)
    path = '/'+'/'.join(parent_dirs)+'/'
    return path + f.name

def folder_id_to_path(folder_id, client=None):
    if client is None:
        client = Client(jwt_auth())
    f = client.file(folder_id).get()
    return path_to_root(f)

def path_to_obj(client, path, type=None):
    target = path.split('/')[-1]
    results = client.search().query(query=target, limit=100, result_type=type, order='relevance')
    results = [f for f in results if f.name == target]
    for f in results:
        full_path = obj_to_path(f)
        if full_path == path:
            return f

    # should never reach this point if you find it
    raise ValueError('Path not found:\n Path: {}'.format(path))

def path_to_fid(path,client):
    f = path_to_obj(path=path,client=client)
    return int(f.id)