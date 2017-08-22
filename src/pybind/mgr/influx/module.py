import json
import sys
import time
from ConfigParser import SafeConfigParser
from collections import defaultdict
from influxdb import InfluxDBClient
from mgr_module import MgrModule
from datetime import datetime
from threading import Event 

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


    def get_latest(self, daemon_type, daemon_name, stat):
        data = self.get_counter(daemon_type, daemon_name, stat)[stat]
        if data:
            return data[-1][1]
        else:
            return 0


    def sum_stat(self, type_inst):
        path = "osd." + type_inst.__str__()
        osdmap = self.get("osd_map")
        value = 0
        for osd in osdmap['osds']:
            osd_id = osd['osd']
            value += self.get_latest("osd", osd_id.__str__(), path.__str__())
        return int(value) 
     

    def get_stat(self, type_inst):
        path = "osd." + type_inst.__str__()
        osdmap = self.get("osd_map")
        data = []
        for osd in osdmap['osds']:
            osd_id = osd['osd']
            metadata = self.get_metadata('osd', "%s" % osd_id)
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
        return data 
               

    def get_sum_stats(self, type_inst):
        point = [{
            "measurement": "ceph_cluster_stats",
            "tags": {
                "mgr_id": self.get_mgr_id(),
                "type_instance": type_inst,
            },
                "time" : datetime.utcnow().isoformat() + 'Z',
                "fields" : {
                    "value": self.sum_stat(type_inst)
                }

        }]
        return point 
            
    
    def get_df_stats(self):
        df = self.get("df")
        data = []
        df_type = [
            'bytes_used',
            'dirty',
            'rd_bytes',
            'raw_bytes_used',
            'wr_bytes',
            'objects',
            'max_avail'
        ]
        for df_type in df_type:
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
            
        
    def send_to_influx(self):
        config = SafeConfigParser()
        config.read('/etc/ceph/influx.conf')
        host = config.get('influx','hostname')
        username = config.get('influx', 'username')
        password = config.get('influx', 'password')
        database = config.get('influx', 'database')
        port = int(config.get('influx','port'))
        stats = config.get('influx', 'stats').replace(' ', '').split(',')
        client = InfluxDBClient(host, port, username, password, database) 
        database_avail = client.get_list_database()
        for database_avail in database_avail:
            if database_avail == database: 
                break
            else: 
                client.create_database(database)

        default= [
            "op_w",
            "op_in_bytes",
            "op_r",
            "op_out_bytes"
        ]

        for stats in stats:
            if stats == "pool":
                client.write_points(self.get_df_stats())

            elif stats == "cluster":
                for default_cluster in default:
                    client.write_points(self.get_sum_stats(default_cluster), 'ms')
                    if config.has_option('extended', 'cluster'):
                        cluster = config.get('extended', 'cluster').replace(' ', '').split(',')
                        for cluster in cluster:
                            client.write_points(self.get_sum_stats(cluster), 'ms')
                self.log.debug("wrote cluster stats")

            elif stats == "osd":
                for default_osd in default: 
                    client.write_points(self.get_stat(default_osd), 'ms')
                    if config.has_option('extended', 'osd'):
                        osd = config.get('extended', 'osd').replace(' ', '').split(',')
                        for osd in osd:
                            client.write_points(self.get_stat(osd), 'ms')
                self.log.debug("wrote osd stats")

            else:
                self.log.error("Invalid stat type config")
            
        self.log.debug("sent all stats to influx")
         
    def shutdown(self):
        self.log.info('Stopping influx module')
        self.run = False
        self.event.set()

    def serve(self):
        self.log.info('Starting influx module')
        self.run = True
        config = SafeConfigParser()
        config.read('/etc/ceph/influx.conf')
        
        while self.run:
            self.send_to_influx()
            self.log.debug("Running interval loop")
            interval = int(config.get('influx','interval'))
            self.log.debug("sleeping for %d seconds",interval)
            self.event.wait(interval)
            
        
    def handle_command(self, cmd):
        if cmd['prefix'] == 'influx self-test':
            self.send_to_influx()
            return 0,' ', 'debugging module'
        else:
            print 'not found'
            raise NotImplementedError(cmd['prefix'])

    
    

       
    

        


