from collections import defaultdict
from datetime import datetime
import json
import sys
from threading import Event
import time
from ConfigParser import SafeConfigParser
from influxdb import InfluxDBClient
from mgr_module import MgrModule

class Module(MgrModule):
    
    COMMANDS = [
        {
            "cmd": "influx self-test",
            "desc": "debug the module",
            "perm": "rw"  
        },

        {   "cmd": "influx config-set name=key,type=CephString "
                "name=value,type=CephString",
            "desc": "Set a configuration value",
            "perm": "rw"
        },
        
        {   "cmd": "influx config-show",
            "desc": "Show configuration value",
            "perm": "r"

        }
    ]


    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.event = Event()
        self.run = True 


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
                point = {
                    "measurement": "ceph_osd_stats",
                    "tags": {
                        "mgr_id": self.get_mgr_id(),
                        "osd_id": osd_id,
                        "type_instance": default,
                        "host": metadata['hostname']
                    },
                        "time" : datetime.utcnow().isoformat() + 'Z', 
                        "fields" : {
                            "value": self.get_latest("osd", osd_id.__str__(), "osd."+ default.__str__())
                        }
                }
                osd_data.append(point)
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
            value += self.get_latest("osd", osd_id.__str__(), path.__str__())
            point = {
                "measurement": "ceph_osd_stats",
                "tags": {
                    "mgr_id": self.get_mgr_id(),
                    "osd_id": osd_id,
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

    

def send_to_all_host(self):
    influx_configs = json.loads(self.get_config("influx_configs"))
    if type(influx_configs) == "list":
        for config in influx_configs:
            host = influx_configs["hostname"]
            if len(config) > 1:
                username = config["username"]
                password = config["password"]
            else:
                username = ""
                password = ""
            self.send_to_influxdb(host,username,password)    
    else:
        host = influx_configs["hostname"]
        if len(influx_configs) > 1:
            username = influx_configs["username"]
            password = influx_configs["password"]
        else:
            username = ""
            password = ""
        self.send_to_influxdb(host,username,password)

    
    def send_to_influxdb(self,host, username, password):
        client = InfluxDBClient(host, self.get_config("port"), username, password, self.get_config("database")) 
        databases_avail = client.get_list_database()
        default_stats = self.get_default_stat()
        database = self.get_config("database")
        stats = self.get_config("stats")
        extended_osd = self.get_config("extended_osd")
        extended_cluster = self.get_config("extended_cluster")
        for database_avail in databases_avail:
            if database_avail == database: 
                break
            else: 
                client.create_database(database)
        for stat in stats:
            if stat == "pool": 
                client.write_points(self.get_df_stats(), 'ms')

            elif stat == "osd":
                client.write_points(default_stats[0], 'ms')
                if extended_osd != "":
                    osds = extended_osd.replace(' ', '').split(',')
                    for osd in osds:
                        client.write_points(self.get_extended("osd", osd), 'ms')
                self.log.debug("wrote osd stats")

            elif stat == "cluster": 
                client.write_points(default_stats[-1], 'ms')
                if extended_cluster != "":
                    clusters = extended_cluster.replace(' ', '').split(',')
                    for cluster in clusters:
                        client.write_points(self.get_extended("cluster", cluster), 'ms')
                self.log.debug("wrote cluster stats")
            else:
                self.log.error("invalid stat")

    def shutdown(self):
        self.log.info('Stopping influx module')
        self.run = False
        self.event.set()

    def handle_command(self, cmd):
        if command['prefix'] == 'influx config-show':
            return 0, json.dumps(self.config), ''
        
        elif command['prefix'] == 'influx config-set':
            key = command['key']
            value = command['value']
            if not value:
                return -errno.EINVAL, '', 'Value should not be empty or None'
            
            self.log.debug('Setting configuration option %s to %s', key, value)
            self.set_localized_config(key, value)
            return 0, 'Configuration option {0} updated'.format(key), ''

        elif command['prefix'] == 'influx self-test':
            self.send_to_all_host()
            return 0, 'Self-test succeeded', ''

        else:
            return (-errno.EINVAL, '',
                    "Command not found '{0}'".format(command['prefix']))

    def serve(self):
        self.log.info('Starting influx module')
        self.run = True
        while self.run:
            self.send_to_all_host()
            self.log.debug("Running interval loop")  
            interval = int(self.get_config("interval"))
            self.log.debug("sleeping for %d seconds",interval)
            self.event.wait(interval)

        


