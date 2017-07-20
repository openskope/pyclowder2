import os
import requests
import sys
import logging
from datasets import get_file_list


class Clowder(object):

    def __init__(self, url=None, auth=None, verify=True):

        self.url = url if url else os.environ.get('CLOWDER_URL','')
        self.auth = auth if auth else self._auth()
        self.verify = verify


    def _auth(self):
    
        login = os.environ.get('CLOWDER_LOGIN', '')
        password = os.environ.get('CLOWDER_PASSWORD', '')
        return (login,password)

    
    def _api(self, endpoint=''):
    
        return os.path.join(self.url, 'api', endpoint)
    
    
    def _get(self, endpoint):
    
        r = requests.get(self._api(endpoint), auth=self.auth, 
                verify=self.verify)
        r.raise_for_status()
       
        return r
    
    
    def _post(self, endpoint, data, files=None):
    
        if files:
            r = requests.post(self._api(endpoint), auth=self.auth,
                files=files, verify=self.verify)
        else:
            r = requests.post(self._api(endpoint), auth=self.auth,
                json=data, verify=self.verify)

        r.raise_for_status()
    
        return r
    
    
    def _put(self, endpoint, data):
    
        r = requests.put(self._api(endpoint), auth=self.auth, json=data, 
                verify=self.verify)
        r.raise_for_status()
    
        return r
    
    
    def _delete(self, endpoint):
    
        r = requests.delete(self._api(endpoint), auth=self.auth, 
                verify=self.verify)
        r.raise_for_status()
       
        return r
    

    def get_dataset_id(self, dataset_name):

        #TODO:In here are we going to assume that 
        #there will be no repeated names?
        dataset_list = self._get('datasets').json()
        for dataset in dataset_list:
            if dataset['name']==dataset_name:
                dataset_id = dataset['id']

        return dataset_id


    def get_file_id(self, dataset_id, file_name):

        file_list = self._get('datasets/{}/listFiles'.format\
                              (dataset_id)).json()
        for f in file_list:
            if f['filename']==file_name:
                file_id = f['id']

        return file_id


    def get_user_id(self, dataset_name):

        dataset_list = self._get('datasets').json()

        for dataset in dataset_list:
            if dataset['name']==dataset_name:
                user_id = 'http://141.142.170.103/api/users/{}'.\
                          format(dataset['authorId'])

        return user_id


    def list_datasets(self):
    
        return self._get('datasets').json()
    
    
    def create_dataset(self, name, description='', filenames=None,
                       parentid=None, spaceid=None):
    
        payload={'name': name, 'description': description}
        if parentid:
            pyaload['collection'] = parentid
        if spaceid:
            payload['spaceid'] = spaceid
    
        dataset_list = self.list_datasets()
        dataname_list = [dataset['name'] for dataset in dataset_list]
        logger = logging.getLogger(__name__)
        if name in dataname_list:
            logger.warning("dataset already exists")
            dataset_id = self.get_dataset_id(name)
            return {'id':dataset_id} 
        if filenames:
            pass
        else:
            return self._post('datasets/createempty', payload).json()


    def delete_dataset(self, dataset):

         r = self._delete('datasets/{}'.format(dataset))
    

    def delete_dataset_name(self, name):
        """delete a dataset by name."""
    
        datasets = self.list_datasets()
        ids = {ds['id']: ds['name'] for ds in datasets}
         
        # count the number datasets with name and delete if there is only one
        count = ids.values().count(name)

        if count == 1:
            self.delete_dataset(ids.keys()[ids.values().index(name)])

        elif count > 1:
            sys.stderr.write('dataset {} found more than once\n'.format(name))
    
        else:
            sys.stderr.write('dataset {} not found\n'.format(name))


    def show_dataset(self, dataset_name):

        dataset_list = self.list_datasets()
        for dataset in dataset_list:
            if dataset['name']==dataset_name:
                authorId = dataset['authorId']
                description =dataset['description']

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
        print('uploaded')


    def delete_dataset_metadata(self, dataset_name):

        dataset_id = self.get_dataset_id(dataset_name)

        r = self._delete('datasets/{}/metadata.jsonld'.format(dataset_id))

        r.raise_for_status()


    def list_file(self, dataset_name):

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
        print('uploaded')


    def delete_file_metadata(self, dataset_name, file_name):

        dataset_id = self.get_dataset_id(dataset_name)
        file_id = self.get_file_id(dataset_id, file_name)

        r = self._delete('files/{}/metadata.jsonld'.format(file_id))

        r.raise_for_status()
