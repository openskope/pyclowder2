

class ClowderBase(object):

    def __init__(self, session, baseurl):
        self.session = session
        self.baseurl = baseurl

    def _get(self, path, stream=False, params=None):
        url = '/'.join((self.baseurl, 'api', path))
        r = self.session.get(url, params=params)
        r.raise_for_status()

        # let the calling function handle streamed data
        if stream:
            return r
        else:
            return r.json()

    def _put(self, path, params=None, data=None):
        url = '/'.join((self.baseurl, 'api', path))
        r = self.session.put(url, params=params, json=data)
        r.raise_for_status()
        return r.json()

    def _post(self, path, params=None, data=None):
        url = '/'.join((self.baseurl, 'api', path))
        r = self.session.post(url, params=params, json=data)
        r.raise_for_status()
        return r.json()

    def _delete(self, path):
        url = '/'.join((self.baseurl, 'api', path))
        r = self.session.delete(url)
        r.raise_for_status()
        return r.json()

