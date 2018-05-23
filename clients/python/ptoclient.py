import codecs
import json
import datetime
import urllib.parse
from collections import deque

import requests
import pandas as pd
import networkx as nx
import dateparser

def _as_time_query_string(v):

    if isinstance(v, int):
        raise ValueError("...no parsing of epoch times yet...")
    elif isinstance(v, str):
        dtv = dateparser.parse(v)
    elif isinstance(v, datetime.datetime):
        dtv = v

    return dtv.strftime("%Y-%m-%dT%H:%M:%SZ")

def _parse_observation_result(j):
    return { 
        'time_start': j[1],
        'time_end': j[2],
        'path': j[3],
        'condition': j[4],
        'value': j[5]
        }

def _parse_set_result(j):
    pass

def _parse_aggregation_result_fn(j, count_label, *groups):
    if len(groups) == 1:
        return lambda j: {groups[0]: j[0], count_label: j[1]}
    elif len(groups) == 2:
        return lambda j: {groups[0]: j[0], groups[1]: j[1], count_label: j[2]}
    else:
        raise RuntimeError("bad group count building aggregation result parse function")


class PTOError(Exception):
    def __init__(self, status, text):
        super().__init__(str(status) + ": " + text)

class PTOQuerySpec:
    """
    Represents a query specification: all the parameters 
    necessary to run a PTO query. Pass to PTOClient to submit 
    this query on a PTO instance.

    """
    def __init__(self):
        super().__init__()

        self._time_start = None
        self._time_end = None
        self._set_ids = []
        self._on_path = []
        self._sources = []
        self._targets = []
        self._conditions = []
        self._group_by = []
        self._options = []

    def time(self, time_start, time_end):
        self._time_start = _as_time_query_string(time_start)
        self._time_end = _as_time_query_string(time_end)
        return self
    
    def set_id(self, *args):
        for v in args:
            if isinstance(v, int):
                # integer, render as hex
                self._set_ids.append("{:x}".format(v))
            elif isinstance(v, str):
                # string, strip possible URL parts
                self._set_ids.append(v.split("/")[-1])
            else:
                raise TypeError("can't deal with {} as set_id".format(v))
        return self

    def on_path(self, *args):
        for v in args:
            self._on_path.append(str(v))
        return self
    
    def source(self, *args):
        for v in args:
            self._sources.append(str(v))
        return self

    def target(self, *args):
        for v in args:
            self._targets.append(str(v))
        return self

    def condition(self, *args):
        for v in args:
            self._conditions.append(str(v))
        return self

    def _append_group(self, gstr):
        if len(self._group_by) >= 2:
            raise ValueError("only one- and two-dimensional grouping is currently supported by the PTO")
        self._group_by.append(gstr)
        return self

    def time_series_year(self):
        return self._append_group("year")

    def time_series_month(self):
        return self._append_group("month")

    def time_series_week(self):
        return self._append_group("week")

    def time_series_day(self):
        return self._append_group("day")

    def time_series_hour(self):
        return self._append_group("hour")

    def group_by_day_of_week(self):
        return self._append_group("week_day")

    def group_by_hour_of_day(self):
        return self._append_group("day_hour")

    def group_by_condition(self):
        return self._append_group("condition")

    def group_by_source(self):
        return self._append_group("source")

    def group_by_target(self):
        return self._append_group("target")
    
    def count_unique_targets(self):
        self._options.append("count_targets")

    def return_sets_only(self):
        self._options.append("sets_only")

    def _params(self):
        params = {}

        params['time_start'] = self._time_start
        params['time_end'] = self._time_end

        if len(self._on_path):
            params['on_path'] = self._on_path
        
        if len(self._sources):
            params['source'] = self._sources
            
        if len(self._targets):
            params['target'] = self._targets

        if len(self._conditions):
            params['condition'] = self._conditions

        if len(self._group_by):
            params['group_by'] = self._group_by

        if len(self._options):
            params['option'] = self._options

        return params

class PTOQuery:
    """
    Represents a specific query in an instance of the PTO.
    """
    def __init__(self, url, token, spec=None):

        super().__init__()
        self._url = url
        self._token = token
        self._metadata = None
        self._results = None

        self._json_to_result_element = None

        if spec is not None:
            self._submit(spec)

    def _submit(self, spec):

        # In the case of submit, URL starts as a base URL
        r = requests.post(self._url+"query/submit",
                          headers = {"Authorization": "APIKEY "+self._token},
                          params=spec.params())
        
        if r.status_code == 200:
            self._metadata = r.json()
            self._url = self._metadata["__link"]
        else:
            raise PTOError(r.status_code, r.text)

    def _determine_query_type(self, qs):
        qd = urllib.parse.parse_qs(qs)
        if "group_by" in qd:
            if "options" in qd and "count_targets" in qd["options"]:
                count_label = "targets"
            else:
                count_label = "observations"
            self._json_to_result_element = _parse_aggregation_result_fn(count_label, *qd["group_by"])
        elif "options" in qd and "sets_only" in qd["options"]:
            self._json_to_result_element = _parse_set_result
        else:
            self._json_to_result_element = _parse_observation_result

    def metadata(self, reload=False):
        if (self._metadata is None) or reload:
            r = requests.get(self._url, headers = {"Authorization": "APIKEY "+self._token})

            if r.status_code == 200:
                self._metadata = r.json()  

            self._determine_query_type(self._metadata["__encoded"])

        return self._metadata
    
    def _reload_http_gen(self, res):
        resin = codecs.getreader("utf8")(res.content)
        
        for line in resin:
            jl = json.loads(line)
            yield self._json_to_result_element(jl)

    def results(self, reload=False):
        """
        Return the results of this query as a Pandas dataframe. 
        If the query is still executing, returns None
        """
        if self._results is None or reload:
        
            m = self._metadata(reload)

            if "__results" not in m:
                return None
            
            r = requests.get(m["__results"], stream=True,
                            headers = {"Authorization": "APIKEY "+self._token})

            if r.status_code == 200:
                self._results = pd.DataFrame(self._reload_http_gen(r))
            else:
                raise PTOError(r.status_code, r.text)
        
        return self._results
    
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
                yield _parse_observation_result(ja)

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
                         headers = {"Authorization": "APIKEY "+self._token})   

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
                 headers = {"Authorization": "APIKEY "+self._token})   
            
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
                         headers = {"Authorization": "APIKEY "+self._token})
        
        if r.status_code == 200:
            return r.json()["sets"]
        else:
            raise PTOError(r.status_code, r.text)
            
    def retrieve_set(self, url=None, setid=None):
        """
        Retrieve a PTOSet with this client by URL or by set ID. 
        Prefetches metadata to force error on nonexistant set.

        """

        if url is None:
            if isinstance(setid, int):
                setid = "{:x}".format(setid)
            url = self._baseurl + "obs/{}".format(str(setid))

        elif not url.startswith(self._baseurl):
            raise ValueError("This client cannot connect to {}",format(url))

        ptoset = PTOSet(url, self._token)
        ptoset.metadata()

        return ptoset
    
    def retrieve_query(self, url=None, queryid=None):
        """
        Retrieve an existing query with this client by URL or query ID.
        Prefetches metadata to force error on nonexistant query.
        Use submit_query instead if the query does not yet exist.
        
        """
        if url is None:
            url = self._baseurl + "query/{}".format(str(queryid))

        elif not url.startswith(self._baseurl):
            raise ValueError("This client cannot connect to {}",format(url))

        q = PTOQuery(url, self._token)
        q.metadata()

        return q

    def submit_query(self, spec):

        q = PTOQuery(self._baseurl, self._token, spec=spec)
        q.metadata()

        return q

def _retrieve_provenance(url, token):

    p = []

    r = requests.get(url, headers = {"Authorization": "APIKEY "+token}) 

    if r.status_code != 200:
        raise PTOError(r.status_code, r.text)

    j = r.json()
    if "_sources" in j:
        p += j["_sources"]
    if "_analyzer" in j:
        p.append(j["_analyzer"])
        
    return p

class Provenance():
    """
    Provenance represents the full provenance of an object: its source and
    analyzer antecedents, back to raw data and analyzers.
    """

    def __init__(self, url, token):
        super().__init__()
        self._url = url
        self._netloc = urllib.parse.urlparse(url).netloc
        self._token = token

        self._edges = {}
        self._errors = []

        self._iterate()

    def _iterate(self):

        url = self._url
        urlq = deque()

        while True:


            try:
                p = _retrieve_provenance(url, self._token)
                print("retrieved {}".format(url))
            except Exception as e:
                self._errors.append(url)
                print("error retrieving {}: {}".format(url, str(e)[:40]))
                continue 

            if len(p) > 0:
                self._edges[url] = p

            for u in p:
                if u not in self._edges:
                    if urllib.parse.urlparse(u).netloc == self._netloc:
                        urlq.append(u)

            if len(urlq) == 0:
                break

            url = urlq.popleft()

    def as_nxgraph(self):
        g = nx.DiGraph()
        for a in self._edges:
            for b in self._edges[a]:
                g.add_edge(a, b)
        
        return g