from datetime import datetime
from threading import Event
import json
import errno
import time

from mgr_module import MgrModule

try:
    from influxdb import InfluxDBClient
    from influxdb.exceptions import InfluxDBClientError
    from influxdb.exceptions import InfluxDBServerError
    from requests.exceptions import ConnectionError
except ImportError:
    InfluxDBClient = None


class Module(MgrModule):
    OPTIONS = [
            {
                'name': 'hostname',
                'default': None
            },
            {
                'name': 'port',
                'default': 8086
            },
            {
                'name': 'database',
                'default': 'ceph'
            },
            {
                'name': 'username',
                'default': None
            },
            {
                'name': 'password',
                'default': None
            },
            {
                'name': 'interval',
                'default': 5
            },
            {
                'name': 'ssl',
                'default': 'false'
            },
            {
                'name': 'verify_ssl',
                'default': 'true'
            },
            {
                'name': 'destinations',
                'default': {}
            }
    ]

    @property
    def config_keys(self):
        return dict((o['name'], o.get('default', None))
                for o in self.OPTIONS)

    COMMANDS = [
        {
            "cmd": "influx config-set name=key,type=CephString "
                   "name=value,type=CephString",
            "desc": "Set a configuration value",
            "perm": "rw"
        },
        {
            "cmd": "influx config-show",
            "desc": "Show current configuration",
            "perm": "r"
        },
        {
            "cmd": "influx send",
            "desc": "Force sending data to Influx",
            "perm": "rw"
        },
        {
            "cmd": "influx self-test",
            "desc": "debug the module",
            "perm": "rw"
        },
        {
            "cmd": "influx dest-add name=hostname,type=CephString "
                   "name=username,req=false,type=CephString "
                   "name=password,req=false,type=CephString "
                   "name=interval,req=false,type=CephString "
                   "name=database,req=false,type=CephString "
                   "name=port,req=false,type=CephString "
                   "name=ssl,req=false,type=CephString "
                   "name=verify_ssl,req=false,type=CephString ",
            "desc": "add destination information ",
            "perm": "rw"
        },
        {
            "cmd": "influx dest-rm name=hostname,type=CephString",
            "desc": "delete destination information ",
            "perm": "rw"
        },
        {
            "cmd": "influx dest-ls",
            "desc": "show destination information ",
            "perm": "r"
        }
    ]

    config_keys = {
        'hostname': None,
        'port': 8086,
        'database': 'ceph',
        'username': None,
        'password': None,
        'interval': 5,
        'ssl': 'false',
        'verify_ssl': 'true',
        'destinations': None
    }

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.event = Event()
        self.run = True
        self.config = dict()
        self.config['destinations'] = []

    def get_fsid(self):
        return self.get('mon_map')['fsid']

    @staticmethod
    def can_run():
        if InfluxDBClient is not None:
            return True, ""
        else:
            return False, "influxdb python module not found"

    def get_latest(self, daemon_type, daemon_name, stat):
        data = self.get_counter(daemon_type, daemon_name, stat)[stat]
        if data:
            return data[-1][1]

        return 0

    def get_df_stats(self):
        df = self.get("df")
        data = []
        pool_info = {}

        now = datetime.utcnow().isoformat() + 'Z'

        df_types = [
            'bytes_used',
            'kb_used',
            'dirty',
            'rd',
            'rd_bytes',
            'raw_bytes_used',
            'wr',
            'wr_bytes',
            'objects',
            'max_avail',
            'quota_objects',
            'quota_bytes'
        ]

        for df_type in df_types:
            for pool in df['pools']:
                point = {
                    "measurement": "ceph_pool_stats",
                    "tags": {
                        "pool_name": pool['name'],
                        "pool_id": pool['id'],
                        "type_instance": df_type,
                        "fsid": self.get_fsid()
                    },
                    "time": now,
                    "fields": {
                        "value": pool['stats'][df_type],
                    }
                }
                data.append(point)
                pool_info.update({str(pool['id']):pool['name']})
        return data, pool_info

    def get_pg_summary(self, pool_info):
        time = datetime.utcnow().isoformat() + 'Z'
        pg_sum = self.get('pg_summary')
        osd_sum = pg_sum['by_osd']
        pool_sum = pg_sum['by_pool']
        data = []
        for osd_id, stats in osd_sum.iteritems():
            metadata = self.get_metadata('osd', "%s" % osd_id)
            for stat in stats:
                point_1 = {
                    "measurement": "ceph_pg_summary_osd",
                    "tags": {
                        "ceph_daemon": "osd." + str(osd_id),
                        "type_instance": stat,
                        "host": metadata['hostname']
                    },
                    "time" : time,
                    "fields" : {
                        "value": stats[stat]
                    }
                }
                data.append(point_1)
        for pool_id, stats in pool_sum.iteritems():
            for stat in stats:
                point_2 = {
                    "measurement": "ceph_pg_summary_pool",
                    "tags": {
                        "pool_name" : pool_info[pool_id],
                        "pool_id" : pool_id,
                        "type_instance" : stat,
                    },
                    "time" : time,
                    "fields": {
                        "value" : stats[stat],
                    }
                }
                data.append(point_2)
        return data


    def get_daemon_stats(self):
        data = []

        now = datetime.utcnow().isoformat() + 'Z'

        for daemon, counters in self.get_all_perf_counters().iteritems():
            svc_type, svc_id = daemon.split(".", 1)
            metadata = self.get_metadata(svc_type, svc_id)

            for path, counter_info in counters.items():
                if counter_info['type'] & self.PERFCOUNTER_HISTOGRAM:
                    continue

                value = counter_info['value']

                data.append({
                    "measurement": "ceph_daemon_stats",
                    "tags": {
                        "ceph_daemon": daemon,
                        "type_instance": path,
                        "host": metadata['hostname'],
                        "fsid": self.get_fsid()
                    },
                    "time": now,
                    "fields": {
                        "value": value
                    }
                })

        return data

    def perfect_config_option(self, option, value):
        if option not in self.config_keys.keys():
            raise RuntimeError('{0} is a unknown configuration '
                               'option'.format(option))

        if option in ['port', 'interval']:
            try:
                value = int(value)
            except (ValueError, TypeError):
                raise RuntimeError('invalid {0} configured. Please specify '
                                   'a valid integer'.format(option))

        if option == 'interval' and value < 5:
            raise RuntimeError('interval should be set to at least 5 seconds')

        if option in ['ssl', 'verify_ssl']:
            value = value.lower() == 'true'

        return value

    def init_module_config(self):
        self.config['hostname'] = \
            self.get_config("hostname", default=self.config_keys['hostname'])
        self.config['port'] = \
            int(self.get_config("port", default=self.config_keys['port']))
        self.config['database'] = \
            self.get_config("database", default=self.config_keys['database'])
        self.config['username'] = \
            self.get_config("username", default=self.config_keys['username'])
        self.config['password'] = \
            self.get_config("password", default=self.config_keys['password'])
        self.config['interval'] = \
            int(self.get_config("interval",
                                default=self.config_keys['interval']))
        ssl = self.get_config("ssl", default=self.config_keys['ssl'])
        self.config['ssl'] = ssl.lower() == 'true'
        verify_ssl = \
            self.get_config("verify_ssl", default=self.config_keys['verify_ssl'])
        self.config['verify_ssl'] = verify_ssl.lower() == 'true'

        num = min([len(x) for x in self.config])

        self.config['destinations'].append({ 'hostname':   self.config['hostname'],
                'username':   self.config['username'],
                'password':   self.config['password'],
                'database':   self.config['database'],
                'ssl':        self.config['ssl'],
                'verify_ssl': self.config['verify_ssl']
        })

        self.init_influx_clients()
    def init_influx_clients(self):
        self.clients = []
        destinations = None
        if not self.config['destinations']:
            destinations = [
                {
                    'hostname':   self.config['hostname'],
                    'username':   self.config['username'],
                    'password':   self.config['password'],
                    'database':   self.config['database'],
                    'ssl':        self.config['ssl'],
                    'verify_ssl': self.config['verify_ssl']
                },
            ]
        else:
            destinations =  self.config['destinations']
            self.log.warn("Test 2: Debug: " + str(destinations))
        for dest in destinations:
            # use global settings if these keys not set in destinations object
            merge_configs = ['port', 'database', 'username', 'password', 'ssl', 'verify_ssl']
            conf = dict()

            for key in merge_configs:
                conf[key] = dest[key] if key in dest else self.config[key]
                # make sure this is an int or may encounter type errors later
                if key == 'port':
                    conf[key] = int(conf[key])
                if key in ['ssl', 'verify_ssl'] and conf[key] is not True:
                    conf[key] = conf[key] == 'true'


            # if not cast to string set_health_check will complain when var is used in error summary string format
            # everything else seems to consider it a string already (?)
            conf['hostname'] = str(dest['hostname'])

            self.log.debug("Sending data to Influx host: %s",
                conf['hostname'])

            client = InfluxDBClient(conf['hostname'], conf['port'],
                conf['username'],
                conf['password'],
                conf['database'],
                conf['ssl'],
                conf['verify_ssl'])

            self.clients.append([client,conf])

    def send_to_influx(self):
        if not self.config['hostname'] and not self.config['destinations']:
            self.log.error("No Influx server configured, please set using: "
                           "ceph influx config-set mgr/influx/hostname <hostname> or ceph influx config-set mgr/influx/destinations '<json array>'")
            self.set_health_checks({
                'MGR_INFLUX_NO_SERVER': {
                    'severity': 'warning',
                    'summary': 'No InfluxDB server configured',
                    'detail': ['Configuration option hostname not set']
                }
            })
            return

        df_stats = self.get_df_stats()
        daemon_stats = self.get_daemon_stats()
        pg_summary = self.get_pg_summary(df_stats[1])

        health = {
            'MGR_INFLUX_SEND_FAILED': {
                'severity': 'warning',
                'summary': "",
                'detail': []
            }
        }
        for client,conf in self.clients:
            # using influx client get_list_database requires admin privs,
            # instead we'll catch the not found exception and inform the user if
            # db can not be created
            error_info = "Hostname: " + conf['hostname'] + ", Port: " + str(conf['port'])
            try:
                client.write_points(df_stats[0], 'ms')
                client.write_points(daemon_stats, 'ms')
                client.write_points(self.get_pg_summary(df_stats[1]))
            except ConnectionError as e:
                # InfluxDBClient also has get_host and get_port but since we have the config here anyways...
                self.log.exception("Failed to connect to Influx host %s:%d", conf['hostname'], conf['port'])
                health['MGR_INFLUX_SEND_FAILED']['severity'] = 'warning'
                health['MGR_INFLUX_SEND_FAILED']['summary'] += 'Timeout sending to InfluxDB server at ' + str(error_info) + " "
                health['MGR_INFLUX_SEND_FAILED']['detail'] += [str(e)]
            except InfluxDBClientError as e:
                if e.code == 404:
                    self.log.info("Database '%s' not found, trying to create "
                                  "(requires admin privs).  You can also create "
                                  "manually and grant write privs to user "
                                  "'%s'", conf['database'],
                                  conf['username'])
                    client.create_database(conf['database'])
                else:
                    health['MGR_INFLUX_SEND_FAILED']['severity'] = 'warning'
                    health['MGR_INFLUX_SEND_FAILED']['summary'] += 'Failed to send data to InfluxDB'
                    health['MGR_INFLUX_SEND_FAILED']['detail'] += [str(e)]
            except InfluxDBServerError as exception:
                self.log.exception("Unable to connect to %s:%d with exception: %s", conf['hostname'], conf['port'], str(exception))
                health['MGR_INFLUX_SEND_FAILED']['summary'] += 'Timeout sending to InfluxDB server at ' + str(error_info) + " "
                health['MGR_INFLUX_SEND_FAILED']['detail'] += [str(exception)]
            except Exception as exception:
                self.log.exception("Unexpected exception: " + str(exception))
                health['MGR_INFLUX_SEND_FAILED']['summary'] += "Unexpected error occurred"
                health['MGR_INFLUX_SEND_FAILED']['detail'] += [str(exception)]
            self.set_health_checks(health)


    def shutdown(self):
        self.log.info('Stopping influx module')
        self.run = False
        self.event.set()

    def handle_command(self, cmd):
        if cmd['prefix'] == 'influx config-show':
            return 0, json.dumps(self.config), ''
        elif cmd['prefix'] == 'influx config-set':
            key = cmd['key']
            value = cmd['value']
            if not value:
                return -errno.EINVAL, '', 'Value should not be empty or None'

            self.log.debug('Setting configuration option %s to %s', key, value)
        elif cmd['prefix'] == 'influx dest-add':
            destination = {}
            is_duplicate = lambda hostname: [dest['hostname'] for dest in self.config['destinations']].__contains__(hostname)
            values = cmd.values()
            for value in values:
                if(value.__contains__("=")):
                    value_split = value.split("=")
                    if not(value_split[1] == "default"):
                        destination[value_split[0]] = self.perfect_config_option(value_split[0], value_split[1])
                else:
                    return 0, "You're not using the proper formatting. Please refer to the docs.", ''
            if(is_duplicate(destination['hostname'])):
                return 0, "You already entered that hostname!", ''
            self.config['destinations'].append(destination)
            self.init_influx_clients()
            self.send_to_influx()
            return 0, "Destination added.", ''
        elif cmd['prefix'] == 'influx dest-rm':
            for dest in self.config['destinations']:
                if(dest['hostname'] == cmd['hostname']):
                    self.config['destinations'].remove(dest)
            self.init_influx_clients()
            self.send_to_influx()
            return 0, "Host: " + cmd['hostname'] + " has been removed.", ''
        elif cmd['prefix'] == 'influx dest-ls':
            returnStr = ""
            for dest in self.config['destinations']:
                returnStr += "Hostname: " + dest['hostname'] + "\n"
                for key in dest:
                    returnStr += ("  " + key + " : " + str(dest[key]) + "\n") if key.__contains__("hostname") is False else ""
                returnStr += "\n"
            return 0, returnStr, ''
        elif cmd['prefix'] == 'influx send':
            self.init_influx_clients()
            self.send_to_influx()
            return 0, 'Sending data to Influx', ''
        if cmd['prefix'] == 'influx self-test':
            daemon_stats = self.get_daemon_stats()
            assert len(daemon_stats)
            df_stats, pools = self.get_df_stats()

            result = {
                'daemon_stats': daemon_stats,
            }

            return 0, json.dumps(result, indent=2), 'Self-test OK'

        return (-errno.EINVAL, '',
                "Command not found '{0}'".format(cmd['prefix']))

    def serve(self):
        if InfluxDBClient is None:
            self.log.error("Cannot transmit statistics: influxdb python "
                           "module not found.  Did you install it?")
            return

        self.log.info('Starting influx module')
        self.init_module_config()
        self.run = True
        while self.run:
            start = time.time()
            self.send_to_influx()
            runtime = time.time() - start
            self.log.debug('Finished sending data in Influx in %.3f seconds',
                           runtime)
            self.log.debug("Sleeping for %d seconds", self.config['interval'])
            self.event.wait(self.config['interval'])
