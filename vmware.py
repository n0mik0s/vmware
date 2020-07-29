import requests
import json
import pprint
import datetime
import es
import re

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class VRealize():
    def __init__(self, base_url, user, password, auth_source, interval_type, interval_quantifier):
        self.pp = pprint.PrettyPrinter(indent=2, width=800)
        self.password = str(password)
        self.user = str(user)
        self.interval_type = str(interval_type)
        self.auth_source = str(auth_source)
        self.base_url = str(base_url)
        self.interval_quantifier = int(interval_quantifier)
        self.timestamp = '{0:%Y-%m-%d %H:%M}'.format(datetime.datetime.utcnow())

    def get_token(self):
        _url = self.base_url + '/suite-api/api/auth/token/acquire'
        _headers = {'Content-Type': 'application/json',
                    'Accept': 'application/json'}
        _payload = {"username": self.user,
                    "password": self.password,
                    "authSource": self.auth_source}

        _response = requests.post(_url,
                                  verify=False,
                                  headers=_headers,
                                  data=json.dumps(_payload))

        _js = json.loads(_response.text)
        if _js['token']:
            return _js['token']
        else:
            self.pp.pprint(_js)
            return False

    def resource_ids(self, auth_token, es_conf):
        _ids = []
        _resorces_all = {}
        _es_conf = es_conf

        _vmware = ['ClusterComputeResource',
                   'ComputeResource',
                   'CustomDatacenter',
                   'Datacenter',
                   'Datastore',
                   'StoragePod',
                   'DatastoreFolder',
                   'VM Entity Status',
                   'Folder',
                   'HostFolder',
                   'HostSystem',
                   'Namespace',
                   'NetworkFolder',
                   'Pod',
                   'ResourcePool',
                   'GuestCluster',
                   'VMwareAdapter Instance',
                   'VirtualMachine',
                   'VMFolder',
                   'DistributedVirtualPortgroup',
                   'VmwareDistributedVirtualSwitch',
                   'vSphere World']
        _vp_san_adapter = ['CacheDisk',
                           'CapacityDisk',
                           'NonVirtualSANDatastore',
                           'VirtualAndPhysicalSANAdapter Instance',
                           'VirtualSANDCCluster',
                           'VirtualSANCluster',
                           'VirtualSANDatastore',
                           'VirtualSANDiskGroup',
                           'VirtualSANFaultDomain',
                           'VirtualSANHost',
                           'VirtualSANWitnessHost',
                           'vSAN World']
        _p_san_adapter = ['PhysicalDisk',
                          'NFSStorage',
                          'NFSVolume',
                          'StorageArray',
                          'StorageProcessor',
                          'PhysicalSANAdapter Instance',
                          'StorageLun']

        _adapter_kind = {'VMWARE': _vmware,
                         'VirtualAndPhysicalSANAdapter': _vp_san_adapter,
                         'PhysicalSANAdapter': _p_san_adapter}

        _authorization = 'vRealizeOpsToken ' + str(auth_token)
        _headers = {"Authorization": _authorization,
                    'Accept': 'application/json'}
        _page = 0

        for _adapter_key in _adapter_kind:
            for _resource_key in _adapter_kind[_adapter_key]:
                _url = self.base_url + '/suite-api/api/adapterkinds/' + _adapter_key +\
                       '/resourcekinds/' + _resource_key + '/resources'
                while True:
                    _params = {'pageSize': 10000,
                               'page': _page}
                    _href = {}
                    _response = requests.get(_url,
                                             verify=False,
                                             params=_params,
                                             headers=_headers)

                    _js = json.loads(_response.text)

                    for _resource in _js['resourceList']:
                        if 'identifier' in _resource:
                            _dict = {'resourceId': _resource['identifier']}

                            if 'resourceKey' in _resource:
                                _dict['resourceKey_adapterKindKey'] = _resource['resourceKey']['adapterKindKey']
                                _dict['resourceKey_name'] = _resource['resourceKey']['name']
                                _dict['resourceKey_resourceKindKey'] = _resource['resourceKey']['resourceKindKey']

                                if _resource['resourceKey']['resourceKindKey'] in 'HostSystem':
                                    _pattern = re.compile(r'^(?P<hostname>.*?)\.(?P<domain>.*?)$')
                                    _match = _pattern.match(_resource['resourceKey']['name'])
                                    if _match:
                                        _match_dict = _match.groupdict()
                                        _dict['resourceKey_name_without_domain'] = _match_dict['hostname']

                            _resorces_all[_resource['identifier']] = _dict

                    for _dict in _js['links']:
                        _href[_dict['name']] = _dict['href']

                    if _href['current'] == _href['last']:
                        break

                    _page += 1

        return _resorces_all

    def latest_stats(self, auth_token, ids, es_conf):
        _es_conf = es_conf
        _ids = ids
        _authorization = 'vRealizeOpsToken ' + str(auth_token)
        _headers = {"Authorization": _authorization,
                    'Accept': 'application/json'}
        _url = self.base_url + '/suite-api/api/resources/stats/latest'

        for _id in _ids.keys():
            _latest_stats = []
            _params = {'metrics': 'true',
                       'currentOnly': 'true',
                       'resourceId': _id}

            _response = requests.get(_url,
                                     verify=False,
                                     params=_params,
                                     headers=_headers)

            _js = json.loads(_response.text)

            try:
                _resourceId = _js['values'][0]['resourceId']
            except Exception as _ex:
                pass
            else:
                for stat in _js['values'][0]['stat-list']['stat']:
                    _key = stat['statKey']['key'].split('|')
                    _timestamps = stat['timestamps'][0]
                    _data = stat['data'][0]
                    _dict = {'resourceId': _resourceId,
                             'timestamp': self.timestamp,
                             'method': 'latest_stats',
                             'timestamps': _timestamps,
                             'key': _key,
                             'data': _data,
                             'resourceKey_adapterKindKey': _ids[_id]['resourceKey_adapterKindKey'],
                             'resourceKey_name': _ids[_id]['resourceKey_name'],
                             'resourceKey_resourceKindKey': _ids[_id]['resourceKey_resourceKindKey']
                             }

                    _latest_stats.append(_dict)

            _es_eng = es.es(es_config=_es_conf)
            _es_eng.bulk_insert(es_config=_es_conf, js_arr=_latest_stats)

        return True

    def alerts(self, auth_token, ids, es_conf):
        _ids = ids
        _es_conf = es_conf
        _alerts = []
        _url = self.base_url + '/suite-api/api/alerts'
        _authorization = 'vRealizeOpsToken ' + str(auth_token)
        _headers = {"Authorization": _authorization,
                    'Accept': 'application/json'}
        _page = 0

        while True:
            _params = {'pageSize': 10000,
                       'page': _page}
            _href = {}
            _response = requests.get(_url,
                                     verify=False,
                                     params=_params,
                                     headers=_headers)

            _js = json.loads(_response.text)

            for _alert in _js['alerts']:
                _alert['timestamp'] = self.timestamp
                _alert['method'] = 'alerts'
                if _alert['resourceId'] in _ids:
                    _alert['resourceKey_name'] = _ids[_alert['resourceId']]['resourceKey_name']
                    _alert['resourceKey_adapterKindKey'] = _ids[_alert['resourceId']]['resourceKey_adapterKindKey']
                    _alert['resourceKey_resourceKindKey'] = _ids[_alert['resourceId']]['resourceKey_resourceKindKey']
                else:
                    _alert['resourceKey_name'] = 'None'
                if _alert['links']:
                    del _alert['links']
                _alerts.append(_alert)

            for _dict in _js['links']:
                _href[_dict['name']] = _dict['href']

            if _href['current'] == _href['last']:
                break

            _page += 1

        _es_eng = es.es(es_config=_es_conf)
        _es_eng.bulk_insert(es_config=_es_conf, js_arr=_alerts)

        return True

    def relationships(self, auth_token, ids, es_conf):
        _es_conf = es_conf
        _ids = ids
        _authorization = 'vRealizeOpsToken ' + str(auth_token)
        _headers = {"Authorization": _authorization,
                    'Accept': 'application/json'}

        _ids_keys = list(_ids.keys())
        for _id in _ids_keys:
            _resourceKey_adapterKindKey = _ids[_id]['resourceKey_adapterKindKey']
            _resourceKey_resourceKindKey = _ids[_id]['resourceKey_resourceKindKey']
            if (_resourceKey_adapterKindKey in 'VMWARE') and\
                    ((_resourceKey_resourceKindKey in 'VirtualMachine') or
                     (_resourceKey_resourceKindKey in 'HostSystem') or
                    (_resourceKey_resourceKindKey in 'Datastore')):
                _relationships = []
                _url = self.base_url + '/suite-api/api/resources/' + _id + '/relationships'
                _page = 0

                while True:
                    _params = {'metrics': 'true',
                               'currentOnly': 'true',
                               'resourceId': _id}
                    _href = {}
                    _response = requests.get(_url,
                                             verify=False,
                                             params=_params,
                                             headers=_headers)

                    _js = json.loads(_response.text)

                    for _item in _js['resourceList']:
                        if _ids[_id]['resourceKey_resourceKindKey'] in 'HostSystem':
                            _resourceKey_name_without_domain = _ids[_id]['resourceKey_name_without_domain']
                        else:
                            _resourceKey_name_without_domain = 'None'
                        _relationships.append({'method': 'relationships',
                                               'timestamp': self.timestamp,
                                               'resourceId': _ids[_id]['resourceId'],
                                               'resourceKey_adapterKindKey': _ids[_id]['resourceKey_adapterKindKey'],
                                               'resourceKey_name': _ids[_id]['resourceKey_name'],
                                               'resourceKey_name_without_domain': _resourceKey_name_without_domain,
                                               'resourceKey_resourceKindKey': _ids[_id]['resourceKey_resourceKindKey'],
                                               'relationship_adapterKindKey': _item['resourceKey']['adapterKindKey'],
                                               'relationship_name': _item['resourceKey']['name'],
                                               'relationship_resourceKindKey': _item['resourceKey']['resourceKindKey']
                        })

                    for _dict in _js['links']:
                        _href[_dict['name']] = _dict['href']

                    if _href['current'] == _href['last']:
                        break

                    _page += 1

                _es_eng = es.es(es_config=_es_conf)
                _es_eng.bulk_insert(es_config=_es_conf, js_arr=_relationships)

        return True