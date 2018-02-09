from datetime import datetime
from threading import Event
import json
import errno
import time

from mgr_module import MgrModule

try:
    from influxdb import InfluxDBClient
    from influxdb.exceptions import InfluxDBClientError
except ImportError:
    InfluxDBClient = None


class Module(MgrModule):
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
    ]

    config_keys = {
        'hostname': None,
        'port': 8086,
        'database': 'ceph',
        'username': None,
        'password': None,
        'interval': 5,
        'destinations': None
    }

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.event = Event()
        self.run = True
        self.config = dict()

    def get_fsid(self):
        return self.get('mon_map')['fsid']

    def get_latest(self, daemon_type, daemon_name, stat):
        data = self.get_counter(daemon_type, daemon_name, stat)[stat]
        if data:
            return data[-1][1]

        return 0

    def get_df_stats(self):
        df = self.get("df")
        data = []

        df_types = [
            'bytes_used',
            'dirty',
            'rd_bytes',
            'raw_bytes_used',
            'wr_bytes',
            'objects',
            'max_avail'
        ]

        mgr_id = self.get_mgr_id()
        pool_info = {}
        for df_type in df_types:
            for pool in df['pools']:
                point = {
                    "measurement": "ceph_pool_stats",
                    "tags": {
                        "pool_name" : pool['name'],
                        "pool_id" : pool['id'],
                        "type_instance" : df_type,
                        "mgr_id" : mgr_id,
                    },
                        "time" : datetime.utcnow().isoformat() + 'Z',
                        "fields": {
                            "value" : pool['stats'][df_type],
                        }
                }
                data.append(point)
                pool_info.update({str(pool['id']):pool['name']})
        return data, pool_info

    def get_pg_summary(self, pool_info):
        osd_sum = self.get('pg_summary')['by_osd']
        pool_sum = self.get('pg_summary')['by_pool']
        mgr_id = self.get_mgr_id()
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
                            "time" : datetime.utcnow().isoformat() + 'Z', 
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
                        "mgr_id" : mgr_id,
                    },
                        "time" : datetime.utcnow().isoformat() + 'Z',
                        "fields": {
                            "value" : stats[stat],
                        }
                }
                data.append(point_2)
        return data 


    def get_daemon_stats(self):
        data = []

        for daemon, counters in self.get_all_perf_counters().iteritems():
            svc_type, svc_id = daemon.split(".")
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
                    "time": datetime.utcnow().isoformat() + 'Z',
                    "fields": {
                        "value": value
                    }
                })

        return data

    def set_config_option(self, option, value):
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

        self.config[option] = value

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
        destinations = self.config_keys['destinations']
        if not destinations:
            self.config['destinations'] = \
                self.get_config("destinations", default=self.config_keys['destinations'])
        else:
            self.config['destinations'] = \
                self.get_config("destinations", eval(self.config_keys['destinations']))
            
    def send_to_influx(self):
        if not self.config['hostname'] and not self.config['destinations']:
            self.log.error("No Influx server configured, please set one using: "
                           "ceph influx config-set hostname <hostname> or ceph influx config-set destinations <destinations>")
            return
        if not self.config['destinations']:
            self.log.debug("Sending data to Influx host: %s",
                    self.config['hostname'])
            client = InfluxDBClient(self.config['hostname'], self.config['port'],
                    self.config['username'],
                    self.config['password'],
                    self.config['database'])

            try:
                df_stats = self.get_df_stats()
                client.write_points(df_stats[0], 'ms')
                client.write_points(self.get_daemon_stats(), 'ms')
                client.write_points(self.get_pg_summary(df_stats[1]))
            except InfluxDBClientError as e:
                if e.code == 404:
                    self.log.info("Database '%s' not found, trying to create "
                            "(requires admin privs).  You can also create "
                            "manually and grant write privs to user "
                            "'%s'", self.config['database'],
                            self.config['username'])
                    client.create_database(self.config['database'])
                else:
                    raise
                    
        else: 
            self.log.error(self.config['destinations'])
            destinations = eval(self.config['destinations'])
            self.log.error(type(destinations))
            for dest in destinations:
                self.log.error(type(dest))
                client = InfluxDBClient(dest['hostname'], int(dest['port']),
                dest['username'], 
                dest['password'], 
                dest['database'] )

                try:
                    df_stats = self.get_df_stats()
                    client.write_points(df_stats[0], 'ms')
                    client.write_points(self.get_daemon_stats(), 'ms')
                    client.write_points(self.get_pg_summary(df_stats[1]))
                except InfluxDBClientError as e:
                    if e.code == 404:
                        self.log.info("Database '%s' not found, trying to create "
                                    "(requires admin privs).  You can also create "
                                    "manually and grant write privs to user "
                                    "'%s'", self.config['database'],
                                    self.config['username'])
                        client.create_database(self.config['database'])
                    else:
                        raise


        # If influx server has authentication turned off then
        # missing username/password is valid.
        # using influx client get_list_database requires admin privs,
        # instead we'll catch the not found exception and inform the user if
        # db can not be created


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
            self.set_config_option(key, value)
            self.set_config(key, value)
            return 0, 'Configuration option {0} updated'.format(key), ''
        elif cmd['prefix'] == 'influx send':
            self.send_to_influx()
            return 0, 'Sending data to Influx', ''
        if cmd['prefix'] == 'influx self-test':
            daemon_stats = self.get_daemon_stats()
            assert len(daemon_stats)
            df_stats = self.get_df_stats()[0]

            result = {
                'daemon_stats': daemon_stats,
                'df_stats': df_stats
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


            
