import codecs
import json

import requests
import pandas as pd

class PTOError(Exception):
    def __init__(self, status, text):
        super().__init__(str(status) + ": " + text)
    
class PTOQuery:
    """
    Represents a specific query in a specific 
    """
    def __init__(self, url, token):
        super().__init__()
        self._url = url
        self._token = token
        
    def metadata(self):
        pass
    
    def results(self):
        """
        Return the results of this query as a Pandas dataframe.
        """
        pass
    
class PTOSet:
    def __init__(self, url=None, token=None, obsfile=None):
        super().__init__()
        
        if url is not None:
            self._url = url
            self._token = token
        elif obsfile is not None:
            self._obsfile = obsfile
        else:
            raise ValueError("PTOSet requires either a url or an obsfile")
            
        self._metadata = None
        self._obsdata = None

    def _reload_file_gen(self):
         for line in self._obsfile:
            try:
                fc = line.strip()[0]
            except IndexError:
                pass
            
            if fc == '{':
                self._metadata = json.loads(line)
            elif fc == '[':
                ja = json.loads(line)
                yield { 
                        'time_start': ja[1],
                        'time_end': ja[2],
                        'path': ja[3],
                        'condition': ja[4],
                        'value': ja[5]
                      }

    def _reload_http_gen(self, res):
        obsin = codecs.getreader("utf8")(res.content)
        
        for line in obsin:
            ja = json.loads(line)
            yield { 
                'time_start': ja[1],
                'time_end': ja[2],
                'path': ja[3],
                'condition': ja[4],
                'value': ja[5]
              }
                
    def _reload_http_metadata(self):
        r = requests.get(self._url,
                         headers = {"Authentication": "APIKEY "+self._token})   

        if r.status_code == 200:
            self._metadata = r.json()
        else:
            raise PTOError(r.status_code, r.text)
        
        
    def metadata(self, reload=False):
        """
        Retrieve and cache metadata associated with this observation set.
        For files, this will also cache all data.
        """
        if (self._metadata is None) or ((self._obsfile is None) and reload):
            if self._obsfile is not None:
                self._obsdata = pd.DataFrame(self._reload_file_gen())
            else:
                self._reload_http_metadata()

        return self._metadata
            
    
    def observations(self, reload=False):
        """
        Return the observations in this set as a Pandas dataframe.
        Caches data from the web unless reload is true.
        """
        
        # cache metadata to get a data link
        md = self.metadata(reload)
        try:
            data_url = md["__data"]
        except KeyError:
            data_url = None
        
        if (self._obsdata is None) or ((self._obsfile is None) and reload):
            r = requests.get(data_url, stream=True,
                 headers = {"Authentication": "APIKEY "+self._token})   
            
            if r.status_code == 200:
                self._obsdata = pd.DataFrame(self._reload_http_gen(r))
                
            else:
                raise PTOError(r.status_code, r.text)   
                
        return self._obsdata
      
class PTOClient:
    
    def __init__(self, baseurl, token):
        super().__init__()
        self._baseurl = baseurl
        self._token = token
    
    def sets_by_metadata(self, k=None, v=None, source=None, analyzer=None):
        params = {}
        
        if k:
            params["k"] = k
        if v:
            params["v"] = v
        if source:
            params["source"] = source
        if analyzer:
            params["analyzer"] = analyzer
        
        r = requests.get(self._baseurl+"obs/by_metadata", params=params,
                         headers = {"Authentication": "APIKEY "+self._token})
        
        if r.status_code == 200:
            return r.json()["sets"]
        else:
            raise PTOError(r.status_code, r.text)
            
    def retrieve_set(self, setid):
        # encapsulate set in a PTOSet object
        
        # then get metadata to make sure it exists
        pass
    
    def retrieve_query(self, queryid):
        pass
    
    def submit_query(self, **kwargs):
        pass
    

class Provenance():
    
    def __init__(self, url):
        super().__init__()
        self._url = url