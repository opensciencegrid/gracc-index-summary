#!/usr/bin/env python
import re
import tabulate
import datetime
import requests

from elasticsearch import Elasticsearch
import ConfigParser

def gb(b):
    return b/1024.0/1024.0/1024.0

def kb(b):
    return b/1024.0


class IndicesReport(object):
    """
    Generates a report of total store size and number of documents
    for groups of indices defined by regex patterns.
    """
    def __init__(self, es_client, groups=[]):
        self.client = es_client
        self.groups = groups

        self.tab = []
        self.others = []

    def _addrow(self, name, num, size, docs):
        try:
            doc_size = float(size)/docs
        except ZeroDivisionError:
            doc_size = 0.0
        self.tab.append([
            name,
            num,
            "%0.1f"%gb(size),
            docs,
            "%0.1f"%kb(doc_size),
            ])

    def run(self):
        """
        Run queries to generate report. Called by report() if not already run.
        """
        r=self.client.indices.stats(metric='docs,store')
        idxs=r['indices'].keys()
        for group in self.groups:
            try:
                g = group['name']
                gre = group['regex']
            except KeyError:
                continue

            gidxs = filter(lambda i: re.match(gre,i),idxs)
            size = 0
            docs = 0
            for i in gidxs:
                size += r['indices'][i]['total']['store']['size_in_bytes']
                docs += r['indices'][i]['primaries']['docs']['count']
                idxs.remove(i)
            self._addrow(g,len(gidxs),size,docs)

        # ungrouped
        size = 0
        docs = 0
        for i in idxs:
            size += r['indices'][i]['total']['store']['size_in_bytes']
            docs += r['indices'][i]['primaries']['docs']['count']
            self.others.append(i)
        self._addrow('other',len(idxs),size,docs)

        # total
        size = r['_all']['total']['store']['size_in_bytes']
        docs = r['_all']['primaries']['docs']['count']
        self._addrow('**total**',len(r['indices']),size,docs)

    def report(self):
        """
        Returns report as string.
        """
        r = ''
        r += self.tabulate()
        r += '\n\nNOTE: Sizes include replicas.'
        r += '\n\n<h3>Other Indices</h3>\n'
        r += '\n<pre>'
        for i in sorted(self.others):
            r += str(i)+'\n'
        r += '\n</pre>'
        return r
                
    def tabulate(self, fmt="html"):
        """
        Return report table as string. See tabulate docs for supported formats:
        https://pypi.python.org/pypi/tabulate
        """
        if len(self.tab) == 0:
            self.run()
        headers=["Index Group","# Indices","Size GB","# Docs","Size/Doc KB"]
        return tabulate.tabulate(self.tab, headers, tablefmt=fmt)
    
html_header='''<style>
#index-table table, th, td {
border: dotted 2px;
padding: 4px;
}
</style>
<div id="index-table">
'''

html_footer='''<p>Last Updated %s</p>
</div>
''' % datetime.datetime.utcnow()

dashboard_header='''
{
"dashboard":
{
  "title": "GRACE Elasticsearch Index Summary",
  "tags": ["grace","monitor"],
  "style": "dark",
  "timezone": "UTC",
  "editable": false,
  "rows": [
    {
      "title": "New row",
      "height": "150px",
      "collapse": false,
      "editable": true,
      "panels": [
        {
          "id": 1,
          "span": 12,
          "editable": true,
          "type": "text",
          "mode": "html",
          "content": "'''

dashboard_footer='''",
          "style": {},
          "title": ""
        }
      ]
    }
  ],
  "nav": [
    {
      "type": "timepicker",
      "collapse": false,
      "enable": true,
      "status": "Stable",
      "time_options": [
        "5m",
        "15m",
        "1h",
        "6h",
        "12h",
        "24h",
        "2d",
        "7d",
        "30d"
      ],
      "refresh_intervals": [
        "5s",
        "10s",
        "30s",
        "1m",
        "5m",
        "15m",
        "30m",
        "1h",
        "2h",
        "1d"
      ],
      "now": true
    }
  ],
  "time": {
    "from": "now-6h",
    "to": "now"
  },
  "templating": {
    "list": []
  },
  "version": 1
},
"overwrite": true
}


'''

if __name__=='__main__':
    client = Elasticsearch()
    groups = [
            {'name':'GRACC OSG Raw', 'regex': r'gracc\.osg\.raw\d-\d'},
            {'name':'GRACC OSG Summary', 'regex': r'gracc\.osg\.summary'},
            {'name':'GRACC OSG Transfer Raw', 'regex': r'gracc\.osg-transfer\.raw\d-\d'},
            {'name':'GRACC OSG ITB Raw', 'regex': r'gracc\.osg-itb\.raw\d-\d'},
            {'name':'GRACC Quarantine', 'regex': r'.*-quarantine$'},
            {'name':'OSG history', 'regex': r'osg-'},
            {'name':'LIGO history', 'regex': r'ligo-'},
            {'name':'Gratia Summary', 'regex': r'gratia.osg.summary'},
            {'name':'GRACC Monitor', 'regex': r'gracc-monitor'},
            {'name':'FIFE', 'regex':r'fife|logstash-fife|lpc'},
            {'name':'Marvel', 'regex': r'\.marvel'},
            {'name':'Kibana', 'regex': r'\.kibana'},
            {'name':'Glidein Logs', 'regex': r'glidein\.logs'},
            {'name':'CVMFS Sync Logs', 'regex': r'cvmfs-sync-logs'},
            {'name':'HTCondor XFer Logs', 'regex':r'^htcondor\-xfer\-stats\-.*'},
            {'name':'XRootD StashCache', 'regex':r'^xrd\-stash.*'}
            ]
    ir = IndicesReport(client,groups)

    to_put = dashboard_header + \
             html_header.replace('\n','\\n').replace('"','\\"') + \
             ir.report().replace('\n','\\n').replace('"','\\"') + \
             html_footer.replace('\n','\\n').replace('"','\\"') + \
             dashboard_footer

    # Read in the configuration to get the bearer token
    config = ConfigParser.ConfigParser()
    config.read("config.ini")
    bearer_token = config.get("auth", "key")

    url = "https://gracc.opensciencegrid.org/api/dashboards/db"
    headers = {"Authorization":"Bearer {0}".format(bearer_token), 
               'Content-type': 'application/json', 'Accept': 'application/json'}
    r = requests.post(url, data=to_put, headers=headers)
    print r
    print r.text
    


