from datetime import datetime
from threading import Event
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
        self.port = config.getint('influx','port')
        self.interval = config.getint('influx','interval')

        if config.has_option('extended', 'osd'):
            self.extended = config.get('extended', 'osd').replace(' ', '').split(',')

        self.init_clients() 

    # returns 2 element list of unix timestamp and counter value
    # example: [1508349272L, 63421644240L] 
    def get_latest(self, daemon_type, daemon_name, stat):
        data = self.get_counter(daemon_type, daemon_name, stat)[stat]
        if data:
            return data[-1][1]
        else:
            return None


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
        timestamp = datetime.utcnow().isoformat() + 'Z'
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
                        "time" : timestamp,
                        "fields": {
                            "value" : pool['stats'][df_type],
                        }
                }
                data.append(point)
                pool_info.update({str(pool['id']):pool['name']})
        
        return data, pool_info


    def get_osd_stats(self):
        defaults = [ 
            "op_w",
            "op_in_bytes",
            "op_r",
            "op_out_bytes"
        ]

        stats = defaults + self.extended
       
        osd_data = []
        for stat in stats:
            osdmap = self.get("osd_map")['osds']
            timestamp = datetime.utcnow().isoformat() + 'Z' 
            for osd in osdmap:
                osd_id = osd['osd']
                metadata = self.get_metadata('osd', "%s" % osd_id)
            
                #if osd_id == 10 and stat == 'op_in_bytes':
                    #test = self.get_counter("osd", str(osd_id), "osd."+ str(stat))
                    #self.log.error(test)
                stat_val = self.get_latest("osd", str(osd_id), "osd."+ str(stat))

                if not stat_val:
                    continue

                #if stat_val == 0 and stat == 'recovery_ops':
                #    self.log.error("OSD {0} returned 0 value for stat {1}".format(osd_id, stat))
                #    continue
                point = {
                    "measurement": "ceph_daemon_stats",
                    "tags": {
                        "ceph_daemon": "osd." + str(osd_id),
                        "type_instance": stat,
                        "host": metadata['hostname']
                    },
                        "time" : timestamp, 
                        "fields" : {
                            "value": stat_val
                        }
                }
                osd_data.append(point)
        return osd_data

    def get_pg_summary(self, pool_info):
        osd_sum = self.get('pg_summary')['by_osd']
        pool_sum = self.get('pg_summary')['by_pool']
        mgr_id = self.get_mgr_id()
        data = []
        timestamp = datetime.utcnow().isoformat() + 'Z'
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
                            "time" : timestamp, 
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
                        "time" : timestamp,
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
            
      
