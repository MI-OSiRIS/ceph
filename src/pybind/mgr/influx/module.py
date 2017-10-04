from collections import defaultdict
from datetime import datetime
import json
import sys
from threading import Event
import time
from ConfigParser import SafeConfigParser
from mgr_module import MgrModule

try:
    from influxdb import InfluxDBClient
    from influxdb.exceptions import InfluxDBClientError
except ImportError:
    InfluxDBClient = None

class Module(MgrModule):
    
    COMMANDS = [
        {
            "cmd": "influx self-test",
            "desc": "debug the module",
            "perm": "rw"  
        },
    ]


    def __init__(self, *args, **kwargs): 
        super(Module, self).__init__(*args, **kwargs)
        self.event = Event()
        self.run = True

        # module-specific config init
        config = SafeConfigParser()
        config.read('/etc/ceph/influx.conf')
        self.clients = []
        self.hosts = config.get('influx','hostname').replace(' ', '').split(',')
        self.username = config.get('influx', 'username')
        self.password = config.get('influx', 'password')
        self.database = config.get('influx', 'database')
        self.port = int(config.get('influx','port'))
        self.stats = config.get('influx', 'stats').replace(' ', '').split(',')
        self.interval = int(config.get('influx','interval'))

        if config.has_option('extended', 'osd'):
            self.osds = config.get('extended', 'osd').replace(' ', '').split(',')

        if config.has_option('extended', 'cluster'):
            self.clusters = config.get('extended', 'cluster').replace(' ', '').split(',')

        self.init_clients() 

    def get_latest(self, daemon_type, daemon_name, stat):
        data = self.get_counter(daemon_type, daemon_name, stat)[stat]
        if data:
            return data[-1][1]
        else:
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

        for df_type in df_types:
            for pool in df['pools']:
                point = {
                    "measurement": "ceph_pool_stats",
                    "tags": {
                        "pool_name" : pool['name'],
                        "pool_id" : pool['id'],
                        "type_instance" : df_type,
                        "mgr_id" : self.get_mgr_id(),
                    },
                        "time" : datetime.utcnow().isoformat() + 'Z',
                        "fields": {
                            "value" : pool['stats'][df_type],
                        }
                }
                data.append(point)
        return data


    def get_default_stat(self):
        defaults= [ 
            "op_w",
            "op_in_bytes",
            "op_r",
            "op_out_bytes"
        ]

        osd_data = []
        cluster_data = []
        for default in defaults:
            osdmap = self.get("osd_map")['osds']
            value = 0
            for osd in osdmap:
                osd_id = osd['osd']
                metadata = self.get_metadata('osd', "%s" % osd_id)
                value += self.get_latest("osd", str(osd_id), "osd."+ str(default))
                if value == 0:
                    continue
                point = {
                    "measurement": "ceph_daemon_stats",
                    "tags": {
                        "ceph_daemon": "osd." + str(osd_id),
                        "type_instance": default,
                        "host": metadata['hostname']
                    },
                        "time" : datetime.utcnow().isoformat() + 'Z', 
                        "fields" : {
                            "value": self.get_latest("osd", osd_id.__str__(), "osd."+ default.__str__())
                        }
                }
                osd_data.append(point)

            if value > 0: 
                point2 = {
                    "measurement": "ceph_cluster_stats",
                    "tags": {
                        "mgr_id": self.get_mgr_id(),
                        "type_instance": default,
                    },
                        "time" : datetime.utcnow().isoformat() + 'Z',
                        "fields" : {
                            "value": value 
                        }
                }
                cluster_data.append(point2)
        return osd_data, cluster_data

    def get_extended(self, counter_type, type_inst):
        path = "osd." + type_inst.__str__()
        osdmap = self.get("osd_map")
        data = []
        value = 0
        for osd in osdmap['osds']: 
            osd_id = osd['osd']
            metadata = self.get_metadata('osd', "%s" % osd_id)
            # this method returns 0 if no data was found, continue and don't build a data point if so
            value += self.get_latest("osd", osd_id.__str__(), path.__str__()) 
            if value == 0:
                continue
            
            point = {
                "measurement": "ceph_daemon_stats",
                "tags": {
                    "ceph_daemon": "osd." + str(osd_id),
                    "type_instance": type_inst,
                    "host": metadata['hostname']
                },
                    "time" : datetime.utcnow().isoformat() + 'Z', 
                    "fields" : {
                        "value": self.get_latest("osd", osd_id.__str__(), path.__str__())
                    }
            }
            data.append(point)
        if counter_type == "cluster":
            if value == 0:
                return []
            else:
                point = [{
                    "measurement": "ceph_cluster_stats",
                    "tags": {
                        "mgr_id": self.get_mgr_id(),
                        "type_instance": type_inst,
                    },
                        "time" : datetime.utcnow().isoformat() + 'Z',
                        "fields" : {
                            "value": value 
                        }
                }]
                return point 
        else:
            return data 

    # we are intentionally coding in the assumption that every database host is using the same user,port, database, etc
    # if that were not true it would be necessary to build a more complex list of dictionaries or a set of indexed lists 
    def init_clients(self):
        self.clients = []
        for host in self.hosts:
            c = InfluxDBClient(host, self.port, self.username, self.password, self.database) 
            self.clients.append(c)  

    def query_datapoints(self):
        default_stats = self.get_default_stat()
        # pg_s = self.get('pg_summary')

        # self.log.info(pg_s)

        for stat in self.stats:
            if stat == "pool": 
                self.send_to_influx(self.get_df_stats())

            elif stat == "osd":
                self.send_to_influx(default_stats[0])
                
                for osd in self.osds:
                    self.send_to_influx(self.get_extended("osd", osd))
                self.log.debug("wrote osd stats")

            elif stat == "cluster": 
                self.send_to_influx(default_stats[-1])
                
                for cluster in self.clusters:
                    self.send_to_influx(self.get_extended("cluster", cluster))

                self.log.debug("wrote cluster stats")
            else:
                self.log.error("invalid stat")

    def send_to_influx(self, points, resolution='ms'):
        if len(points) > 0:        
            # catch the not found exception and inform the user, try to create db if we can
            try:
                for client in self.clients:
                    client.write_points(points, resolution)
            except InfluxDBClientError as e:
                if e.code == 404:
                    self.log.info("Database '{0}' not found, trying to create (requires admin privs).  You can also create manually and grant write privs to user '{1}'".format(database,username))
                    client.create_database(database)
                else:
                    raise


    def shutdown(self):
        self.log.info('Stopping influx module')
        self.run = False
        self.event.set()

    def handle_command(self, cmd):
        if cmd['prefix'] == 'influx self-test':
            self.send_to_influx()
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
        self.run = True
        while self.run:
            self.query_datapoints()
            self.log.debug("Running interval loop")
            self.log.debug("sleeping for %d seconds",self.interval)
            self.event.wait(self.interval)
            
