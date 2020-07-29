import argparse
import os
import pprint
import yaml
import datetime
import es
import vmware

if __name__=="__main__":
    startTime = datetime.datetime.now()
    pp = pprint.PrettyPrinter(indent=4)

    argparser = argparse.ArgumentParser(usage='%(prog)s [options]')
    argparser.add_argument('-c', '--conf',
                           help='Set full path to the configuration file.',
                           default='conf.yml')
    argparser.add_argument('-m', '--mode',
                           help='Set mode that should be executed: alerts|stats|relationships.',
                           default='alerts')
    argparser.add_argument('-v', '--verbose',
                           help='Set verbose run to true.',
                           action='store_true')

    args = argparser.parse_args()

    verbose = args.verbose
    mode = args.mode
    root_dir = os.path.dirname(os.path.realpath(__file__))
    conf_path_full = str(root_dir) + os.sep + str(args.conf)

    with open(conf_path_full, 'r') as reader:
        try:
            cf = yaml.safe_load(reader)
        except yaml.YAMLError as ex:
            print('ERR: [main]', ex)
            exit(1)
        else:
            if verbose: pp.pprint(cf)

            session = vmware.VRealize(base_url=cf['vrealize']['base_url'],
                                      user=cf['vrealize']['user'],
                                      password=cf['vrealize']['password'],
                                      auth_source=cf['vrealize']['auth_source'],
                                      interval_type=cf['vrealize']['interval_type'],
                                      interval_quantifier=cf['vrealize']['interval_quantifier'])
            token = session.get_token()

            if token:
                if mode in 'alerts':
                    ids = session.resource_ids(auth_token=token, es_conf=cf['es_config'])
                    session.alerts(auth_token=token, ids=ids, es_conf=cf['es_config'])
                elif mode in 'stats':
                    ids = session.resource_ids(auth_token=token, es_conf=cf['es_config'])
                    session.latest_stats(auth_token=token, ids=ids, es_conf=cf['es_config'])
                elif mode in 'relationships':
                    ids = session.resource_ids(auth_token=token, es_conf=cf['es_config'])
                    session.relationships(auth_token=token, ids=ids, es_conf=cf['es_config'])

    print(datetime.datetime.now() - startTime)