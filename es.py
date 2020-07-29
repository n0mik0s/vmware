import elasticsearch
import elasticsearch.helpers
import json

class es():
    def __init__(self, es_config):
        _es_config = es_config

        try:
            if _es_config['use_ssl']:
                self.es_eng = elasticsearch.Elasticsearch(
                    _es_config['nodes'],
                    port=_es_config['port'],
                    http_auth=(_es_config['user'] + ':' + _es_config['password']),
                    verify_certs=_es_config['verify_certs'],
                    use_ssl=_es_config['use_ssl'],
                    ca_certs=_es_config['ca_cert']
                )
            else:
                self.es_eng = elasticsearch.Elasticsearch(
                    _es_config['es_nodes'],
                    port=_es_config['port'],
                    http_auth=(_es_config['user'] + ':' + _es_config['password'])
                )
        except Exception as _exc:
            print('ERR: [es:__init__]: Error with establishing connection with elastic cluster:', _exc)
            self.es_eng = False

    def bulk_insert(self, es_config, js_arr):
        _es_config = es_config
        _js_arr = js_arr
        _shards = _es_config['shards']
        _replicas = _es_config['replicas']
        _template = _es_config['template']
        _lifecycle_name = _es_config['lifecycle_name']
        _lifecycle_rollover_alias = _es_config['lifecycle_rollover_alias']
        _index = _es_config['index']

        _map = {
            "mappings": {
                "dynamic_templates": [
                    {
                        "resourceKey_as_keyword": {
                            "match": "resourceKey_*",
                            "mapping": {
                                "type": "keyword"
                            }
                        }
                    },
                    {
                        "relationship_as_keyword": {
                            "match": "relationship_*",
                            "mapping": {
                                "type": "keyword"
                            }
                        }
                    }
                ],
                "properties": {
                    "summary_guest_ipAddress": {"type": "ip"},
                    "cancelTimeUTC": {"type": "date", "format": "epoch_millis"},
                    "startTimeUTC": {"type": "date", "format": "epoch_millis"},
                    "updateTimeUTC": {"type": "date", "format": "epoch_millis"},
                    "timestamps": {"type": "date", "format": "epoch_millis"},
                    "timestamp": {"type": "date", "format": "yyyy-MM-dd' 'HH:mm"},
                    "alertImpact": {"type": "keyword"},
                    "alertLevel": {"type": "keyword"},
                    "resourceId": {"type": "keyword"},
                    "status": {"type": "keyword"},
                    "alertDefinitionId": {"type": "keyword"},
                    "controlState": {"type": "keyword"},
                    #"resourceKey_resourceKindKey": {"type": "keyword"},
                    "method": {"type": "keyword"},
                    "key": {"type": "keyword"},
                    #"relationship_adapterKindKey": {"type": "keyword"},
                    #"relationship_name": {"type": "keyword"},
                    #"relationship_resourceKindKey": {"type": "keyword"},
                    #"resourceKey_name_without_domain": {"type": "keyword"},
                    # "": {"type": "keyword"},
                    # "": {"type": "keyword"},
                    "data": {"type": "float"},
                }
            }
        }

        _body_index = {
            "aliases": {_lifecycle_rollover_alias: {"is_write_index" : 'true'}}
        }

        _body_template = {
            "index_patterns": [(str(_es_config['pattern']) + '*')],
            "settings": {
                "number_of_shards": _shards,
                "number_of_replicas": _replicas,
                "index.lifecycle.name": _lifecycle_name,
                "index.lifecycle.rollover_alias": _lifecycle_rollover_alias
            },
            "mappings": _map["mappings"],
        }

        _actions = [
            {
                "_index": _lifecycle_rollover_alias,
                "_source": json.dumps(_js)
            }
            for _js in _js_arr
        ]

        if self.es_eng:
            if not self.es_eng.indices.exists_template(name=_template):
                try:
                    self.es_eng.indices.put_template(name=_template, body=_body_template)
                except Exception as _err:
                    print('ERR: [es:bulk_insert]', _err)
                    return False

            if not self.es_eng.cat.aliases(name=_lifecycle_rollover_alias, format='json'):
                try:
                    self.es_eng.indices.create(index=_index, body=_body_index)
                except Exception as _err:
                    print('ERR: [es:bulk_insert]', _err)
                    return False

            try:
                elasticsearch.helpers.bulk(self.es_eng, _actions, chunk_size=500, request_timeout=30)
            except Exception as _err:
                print('ERR: [es:bulk_insert]', _err)
                return False
            else:
                return True