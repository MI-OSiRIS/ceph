from datetime import datetime
from threading import Event
import json
import errno
import time

from mgr_module import MgrModule

try:
    from influxdb import InfluxDBClient
    from influxdb.exceptions import InfluxDBClientError
    from requests.exceptions import ConnectionError
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
        'ssl': 'false',
        'verify_ssl': 'true'
        'destinations': None
    }

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.event = Event()
        self.run = True
        self.config = dict()

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

        mgr_id = self.get_mgr_id()
        pool_info = {}
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
                    "time": datetime.utcnow().isoformat() + 'Z',
                    "fields": {
                        "value": pool['stats'][df_type],
                    }
                }
                data.append(point)
                pool_info.update({str(pool['id']):pool['name']})
        
        return data, pool_info

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
                        "time" : datetime.utcnow().isoformat() + 'Z', 
                        "fields" : {
                            "value": stat_val
                        }
                }
                osd_data.append(point)
        return osd_data

    # we are intentionally coding in the assumption that every database host is using the same user,port, database, etc
    # if that were not true it would be necessary to build a more complex list of dictionaries or a set of indexed lists 
    def get_pg_summary(self, pool_info):
        osd_sum = self.get('pg_summary')['by_osd']
        pool_sum = self.get('pg_summary')['by_pool']
        mgr_id = self.get_mgr_id()
        data = []
        for osd_id, stats in osd_sum.iteritems():
            metadata = self.get_metadata('osd', "%s" % osd_id)
            for stat in stats:
                point_1 = {
                    "measurement": "ceph_osd_summary",
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
                    "measurement": "ceph_pool_stats",
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

        
    def init_clients(self):
        self.clients = []
        for host in self.hosts:
            c = InfluxDBClient(host, self.port, self.username, self.password, self.database) 
            self.clients.append(c)  

    def send_to_influx(self, points, resolution='u'):
        if len(points) > 0:        
            # catch the not found exception and inform the user, try to create db if we can
            try:
                value = int(value)
            except (ValueError, TypeError):
                raise RuntimeError('invalid {0} configured. Please specify '
                                   'a valid integer'.format(option))

        if option == 'interval' and value < 5:
            raise RuntimeError('interval should be set to at least 5 seconds')

        if option in ['ssl', 'verify_ssl']:
            value = value.lower() == 'true'

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
        ssl = self.get_config("ssl", default=self.config_keys['ssl'])
        self.config['ssl'] = ssl.lower() == 'true'
        verify_ssl = \
            self.get_config("verify_ssl", default=self.config_keys['verify_ssl'])
        self.config['verify_ssl'] = verify_ssl.lower() == 'true'


    def send_to_influx(self):
        df_stats = self.get_df_stats()
        daemon_stats = self.get_daemon_stats()
        pg_summary = self.get_pg_summary(df_stats[1])

        self.log.error(daemon_stats)

        for client in self.clients:
            try:
                client.write_points(df_stats[0], 'ms')
                client.write_points(daemon_stats, 'ms')
                client.write_points(pg_summary)
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
            df_stat = self.get_df_stats()
            self.send_to_influx(df_stat[0])
            self.send_to_influx(self.get_osd_stats())
            self.send_to_influx(self.get_pg_summary(df_stat[1]))
            return 0,' ', 'debugging module'
        else:
            print('not found')
            raise NotImplementedError(cmd['prefix'])

    def serve(self):
        if InfluxDBClient is None:
            self.log.error("Cannot transmit statistics: influxdb python "
                           "module not found.  Did you install it?")
            return
        self.log.info('Starting influx module')
        # delay startup 10 seconds, otherwise first few queries return no info
        self.event.wait(10)
        while self.run:
            df_stat = self.get_df_stats()
            self.send_to_influx(df_stat[0])
            self.send_to_influx(self.get_osd_stats())
            self.send_to_influx(self.get_pg_summary(df_stat[1]))
            self.log.debug("Running interval loop")
            self.log.debug("sleeping for %d seconds",self.interval)
            self.event.wait(self.interval)
            
      
