import os
import requests
import sys

import logging
log = logging.getLogger(__name__)

from .datasets import Datasets


class Clowder(object):
    """Convience class for accessing the Clowder API.

    The primary purpose of the Clowder class to maintain connection 
    variables so that arugments (host, login, password, key, etc.)
    do not need to be passed during each function call. Similarly, the 
    class also uses Session from the requests module to allow multiple
    requests to re-use the same TCP connection. In some cases this can 
    significantly enhance performance.

    The Clowder class can be configured to use either basic authentication 
    using login and password or the Clowder key depending on initialization 
    parameters.

    The class also provides functions at a somewhat higher level, operating 
    on names rather than IDs. The higher level methods will attempt to 
    automatically map names to IDs where possible.

    Attributes:
        url (str): base URL to the clowder service
        login (str): login (optional if using key attribute for access)
        password (str): password (optional if using key attribute for access)
        key (str): optional - the Clowder key used as an alternate form
            of authentication
    """

    def __init__(self, url, login=None, password=None, key=None, 
                 ssl_verify=True):
        """Initialize Clowder instance.
        
        Args:
            url (str): The basename of the CLOWDER API endpoint
            login (str): optional user login
            password (str): optional user password
            key (str): optional Clowder key for automatic access
            ssl_verify (bool): SSL verification (default=True)
        """

        self.url = url 
        self.login = login
        self.password = password
        self.key = key

        self._users = {}

        self.session = requests.Session()
        self.session.verify = ssl_verify
        self.session.auth = (self.login, self.password)

        # Adds key in params for the session if provided. Additional params 
        # can still be added in the individual method calls.
        if self.key and not self.login and not self.password:
            self.session.params = { 'key': self.key }
        
        self.datasets = Datasets(self.session, self.url)
    

    def _api(self, endpoint=''):
    
        return os.path.join(self.url, 'api', endpoint.lstrip('/'))


    def _get(self, endpoint, params=None):

        r = self.session.get(self._api(endpoint), params=params)
        r.raise_for_status()
        return r

    
    def _post(self, endpoint, data=None, files=None, params=None):

        if not data and not files:
            raise RuntimeError('data or file list required for HTTP post')

        if files:
            r = self.session.post(self._api(endpoint), params=params, files=files)

        else:
            r = self.session.post(self._api(endpoint), params=params, json=data)

        r.raise_for_status()
        return r
    
    
    def _put(self, endpoint, data, params=None):

        r = self.session.put(self._api(endpoint), params=self.keyparam, json=data)
        r.raise_for_status()
        return r
    
    
    def _delete(self, endpoint, params=None):

        r = self.session.delete(self._api(endpoint), params=self.keyparam)
        r.raise_for_status()
        return r


    #### Collection Methods ####
    
    
    def get_collection_datasets(self, collectionid):
        """Return a list of datasets."""

        return self._get('collections/{}/datasets'.format(collectionid).json())


    #### Dataset Methods ####

    def list_datasets(self):
        """Return a list of datasets."""

        return self._get('datasets').json()


    def create_dataset(self, name, description='', parentid=None, spaceid=None):
        """Create a dataset."""
    
        payload={'name': name, 'description': description}
        if parentid:
            payload['collection'] = parentid
        if spaceid:
            payload['spaceid'] = spaceid
    
        return self._post('datasets/createempty', payload).json()

    def delete_dataset(self, dataset_id):
        """Delete a dataset."""

        r = self._delete('datasets/{}'.format(dataset_id))

    def get_dataset_info(self, dataset_id):
        """Return basic dataset information from UUID."""

        return self._get('datasets/{}'.format(dataset_id)).json()

    def get_dataset_file_list(self, dataset_id):
        """Return list of files in the dataset."""

        return self._get('datasets/{}/files'.format(dataset_id).json())

    def upload_metadata_dataset(self, dataset_id, metadata):
        """Upload dataset JSON-LD metadata."""

        self._post('datasets/{}/metadata.jsonld'.format(dataset_id), json=metadata)

    def download_metadata_dataset(self, dataset_id, extractor=None):
        """Download dataset JSON-LD metadata."""

        params = {}
        if extractor:
            params['extractor'] = extractor

        r = self._get('datasets/{}/metadata.jsonld'.format(dataset_id), params=params)
        return r.json()

    def remove_metadata_dataset(self, dataset_id, extractor=None):
        """Delete dataset JSON-LD metadata from Clowder.

        Warning: ALL METADATA WILL BE REMOVED IF NO extractor IS PROVIED!
        """

        params = {}
        if extractor:
            params['extractor'] = extractor

        self._delete('datasets/{}/metadata.jsonld'.format(dataset_id), params=params)


    ### File Methods ###

    def get_file_info(self, file_id):
        """Return file summary metadata from Clowder."""

        return self._get('files/%s/metadata'.format(file_id)).json()

    def download_file(self, file_id, filename):
        """Download file to a local file."""

        r = self._get('files/{}'.format(file_id), stream=True)

        try:
            with open(filename, 'w') as f:
                for chunk in r.iter_content(chunk_size=10*1024):
                    f.write(chunk)
        except:
            os.remove(filename)
            raise

    def upload_metadata_file(self, file_id, metadata):
        """Upload dataset JSON-LD metadata."""

        self._post('datasets/{}/metadata.jsonld'.format(file_id), json=metadata)

    def download_metadata_file(self, file_id, extractor=None):
        """Download file JSON-LD metadata from Clowder."""

        params = {}
        if extractor:
            params['extractor'] = extractor

        return self._get('files/{}'.format(file_id)).json()

    def remove_metadata_file(self, file_id, extractor=None):
        """Delete file JSON-LD metadata from Clowder.

        Warning: ALL METADATA WILL BE REMOVED IF NO extractor IS PROVIED!
        """

        params = {}
        if extractor:
            params['extractor'] = extractor

        self._delete('files/{}/metadata.jsonld'.format(file_id), params=params)


    # TODO returns the first if several matching dataset names are found
    def get_dataset_id(self, dataset_name):
        """Return the dataset id based on name."""

        dataset_list = self._get('datasets').json()
        for dataset in dataset_list:
            if dataset['name']==dataset_name:
                return dataset['id']

        return None


    def get_file_id(self, dataset_id, file_name):
        """Return the file id  """

        file_list = self._get('datasets/{}/listFiles'.format\
                              (dataset_id)).json()
        for f in file_list:
            if f['filename']==file_name:
                return f['id']

        return None


    def get_user_info(self, _id):
        """Return the user information.

        Note: maintains a cache of user ids as they are found and only queries
        Clowder for unknown user id's.
        """ 

        if _id not in self._users.keys():
            self._users[_id] = self._get('users/{}'.format(_id)).json()

        return self._users[_id]

    
    def delete_dataset_name(self, name):
        """delete a dataset by name."""
    
        ids = {ds['id']: ds['name'] for ds in self.list_datasets()}
         
        # count the number datasets with name and delete if there is only one
        count = ids.values().count(name)

        if count == 1:
            self.delete_dataset(ids.keys()[ids.values().index(name)])

        elif count > 1:
            raise RuntimeError('multiple datasets found with matching name')
    
        else:
            raise RuntimeError('dataset {} not found'.format(name))


    def get_author_info(self, author_id):
        return self._get('users/{}'.format(author_id)).json()


    # TODO move to CLI tool
    def show_dataset(self, dataset_name):
        """print a summary of the dataset(s)."""

        datasets = [ds for ds in self.list_datasets() if ds['name'] == dataset_name]
        if not datasets:
            return

        authors = {}
        for ds in self.list_datasets():
            authorId = ds['authorId']
            description = ds['description']
       

        author_info = self._get('users/{}'.format(authorId)).json()
        author_name = author_info['fullName']

        metadata = self.list_dataset_metadata(dataset_name)

        print('Dataset name: {}'.format(dataset_name))
        print('Description: {}'.format(description))
        print('Author: {}'.format(author_name))
        print('\nMetadata----')
        for data in metadata:
            d = data['content'].items()
            for key,value in d:
                print('{}: {}'.format(key,value))
    

    def list_dataset_metadata(self, dataset_name):

        dataset_id = self.get_dataset_id(dataset_name)

        metadata = self._get('datasets/{}/metadata.jsonld'.format(dataset_id))

        return metadata.json()

   
    def add_dataset_metadata(self, dataset_name, metadata):

        dataset_id = self.get_dataset_id(dataset_name)

        metadata['agent']['user_id'] = self.get_user_id(dataset_name)

        r = self._post('datasets/{}/metadata.jsonld'.format(dataset_id),
                   metadata)


    def delete_dataset_metadata(self, dataset_name):

        dataset_id = self.get_dataset_id(dataset_name)

        r = self._delete('datasets/{}/metadata.jsonld'.format(dataset_id))

        r.raise_for_status()


    def list_files(self, dataset_name):

        dataset_id = self.get_dataset_id(dataset_name)
        file_list = self._get('datasets/{}/listFiles'.format(dataset_id))
        
        return file_list.json()


    def add_file(self, dataset_name, file_path):

        data = {'name': file_path}
        dataset_id = self.get_dataset_id(dataset_name)
        files = {'File': open(file_path, 'rb')}
        r = self._post('uploadToDataset/{}'.format(dataset_id), data, files)


    def delete_file(self, dataset_name, file_name):

        dataset_id = self.get_dataset_id(dataset_name)
        file_id = self.get_file_id(dataset_id, file_name)

        r = self._delete('datasets/{}/{}'.format(dataset_id, file_id))


    # TODO move to CLI tool
    def show_file(self, dataset_name, file_name):

        dataset_id = self.get_dataset_id(dataset_name)
        file_id = self.get_file_id(dataset_id, file_name)
        file_info = self._get('files/{}/metadata'.format(file_id)).json()

        authorId = file_info['authorId']
        author_info = self._get('users/{}'.format(authorId)).json()
        author_name = author_info['fullName']
        description = file_info['filedescription']

        metadata = self.list_file_metadata(dataset_name, file_name)

        print('Dataset name: {}'.format(file_name))
        print('Description: {}'.format(description))
        print('Author: {}'.format(author_name))
        print('\nMetadata----')
        for data in metadata:
            d = data['content'].items()
            for key,value in d:
                print('{}: {}'.format(key,value))


    def list_file_metadata(self, dataset_name, file_name):

         dataset_id = self.get_dataset_id(dataset_name)
         file_id = self.get_file_id(dataset_id, file_name)

         metadata = self._get('files/{}/metadata.jsonld'.format(file_id))

         return metadata.json()


    def add_file_metadata(self, dataset_name, file_name, metadata):

        dataset_id = self.get_dataset_id(dataset_name)
        file_id = self.get_file_id(dataset_id, file_name)

        metadata['agent']['user_id'] = self.get_user_id(dataset_name)

        r = self._post('files/{}/metadata.jsonld'.format(file_id),
                       metadata) 


    def delete_file_metadata(self, dataset_name, file_name):

        dataset_id = self.get_dataset_id(dataset_name)
        file_id = self.get_file_id(dataset_id, file_name)

        r = self._delete('files/{}/metadata.jsonld'.format(file_id))

        r.raise_for_status()
