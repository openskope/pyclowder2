import os
import requests
from files import get_file_list


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
    
    
    def _post(self, endpoint, data):
    
        r = requests.post(self._api(endpoint), auth=self.auth, json=data, 
                verify=self.verify)
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

        dataset_list = requests.get(os.environ.get\
                                    ('CLOWDER_API_BASE')\
                                    + 'datasets').json()
        for data in dataset_list:
            if data['name']==dataset_name:
                dataset_id = data['id']

        return dataset_id


    def get_file_id(self, dataset_id, file_name):
        file_list = get_file_list(None, host, key, dataset_id)
        for f in file_list:
            if f['filename']==file_name:
                file_id = f['id']

        return file_id


    def list_datasets(self):
    
        return self._get('datasets').json()
    
    
    def create_dataset(self, name, description='', filenames=None,
                       parentid=None, spaceid=None):
    
        payload={'name': name, 'description': description}
        if parentid:
            pyaload['collection'] = parentid
        if spaceid:
            payload['spaceid'] = spaceid
    
        # TODO
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
            raise RuntimeError('dataset {} found more than once'.format(name))
    
        else:
            raise RuntimeError('dataset {} not found'.format(name))
    
    
    def add_dataset_metadata(dataset, metadata):
    
        return get('datasets/{}'.format(dataset)).json()
