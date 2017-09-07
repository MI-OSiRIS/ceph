=============
Influx Plugin 
=============

The influx plugin continuously collects and sends time series data to an influxdb database. Users have the option to specify what type of stats they want to collect. 
Some default counters are already set. However, users will have the option to choose some additional counters to collect. 

-------------
Configuration 
-------------

In order for this module to work, configuration keys must be set ``ceph config-key set <key> <value>``. 

^^^^^^^^
Required 
^^^^^^^^

The following configuration keys are required:

- mgr/influx/influx_configs

    Since this module may be running on more than one host, this configuration takes the hostname, username, and password for each host. It also has the ability to take in more than one host at a time. 
    Example:

    ``ceph config-set influx_configs  [{host1,username1,password1},{host2,username2,password2},{host3,username3,password3}]``



- mgr/influx/interval 

- mgr/influx/stats

    Users have the ability to collect stats about either the osd, cluster, or pool. If more than one stat is desired, separate each additional stat with a comma. 
    Example:

    ``ceph config-set stats osd,cluster,pool``

- mgr/influx/ports


^^^^^^^^
Optional 
^^^^^^^^

Users have the ability to collect additional counters for each osd or each cluster by adding a list of counters under the following configuration key:

- mgr/influx/extended_cluster 

- mgr/influx/extended_osd
--------
Enabling 
--------

To enable the module, the following should be performed:

- Load module by including this in the ceph.conf file.::

    [mgr]
        mgr_modules = influx  

- Initialize the module to run every set interval  ``ceph mgr module enable influx``.

---------
Disabling
---------

``ceph mgr module disable influx``

---------
Debugging 
---------

By default, a few debugging statements as well as error statements have been set to print in the log files. Users can add more if necessary.
To make use of the debugging option in the module:

- Add this to the ceph.conf file.::

    [mgr]
        debug_mgr = 20  

- Use this command ``ceph tell mgr.<mymonitor> influx self-test``.
- Check the log files. Users may find it easier to filter the log files using *mgr[influx]*.

-----
Usage
-----

^^^^^^^^^^^^^^^^
Default Counters
^^^^^^^^^^^^^^^^

**pool** 

+---------------+-----------------------------------------------------+
|Counter        | Description                                         |
+===============+=====================================================+
|bytes_used     | Bytes used in the pool not including copies         |
+---------------+-----------------------------------------------------+
|max_avail      | Max available number of bytes in the pool           |
+---------------+-----------------------------------------------------+
|objects        | Number of objects in the pool                       |
+---------------+-----------------------------------------------------+
|wr_bytes       | Number of bytes written in the pool                 |
+---------------+-----------------------------------------------------+
|dirty          | Number of bytes dirty in the pool                   |
+---------------+-----------------------------------------------------+
|rd_bytes       | Number of bytes read in the pool                    |
+---------------+-----------------------------------------------------+
|raw_bytes_used | Bytes used in pool including copies made            |
+---------------+-----------------------------------------------------+

**osd**

+------------+------------------------------------+
|Counter     | Description                        |
+============+====================================+
|op_w        | Client write operations            |
+------------+------------------------------------+
|op_in_bytes | Client operations total write size |
+------------+------------------------------------+
|op_r        | Client read operations             |
+------------+------------------------------------+
|op_out_bytes| Client operations total read size  |
+------------+------------------------------------+


**cluster**
The cluster will collect the same type of data as the osd by default but instead of collecting per osd, it will sum up the performance counter 
for all osd.

^^^^^^^^
extended
^^^^^^^^
There are many other counters that can be collected by configuring the module such as operational counters and suboperational counters. A couple of counters are listed and described below, but additional counters 
can be found here https://github.com/ceph/ceph/blob/5a197c5817f591fc514f55b9929982e90d90084e/src/osd/OSD.cc

**Operations**

- Latency counters are measured in microseconds unless otherwise specified in the description.

+------------------------+--------------------------------------------------------------------------+
|Counter                 | Description                                                              |
+========================+==========================================================================+
|op_wip                  | Replication operations currently being processed (primary)               |
+------------------------+--------------------------------------------------------------------------+
|op_latency              | Latency of client operations (including queue time)                      |
+------------------------+--------------------------------------------------------------------------+
|op_process_latency      | Latency of client operations (excluding queue time)                      |           
+------------------------+--------------------------------------------------------------------------+
|op_prepare_latency      | Latency of client operations (excluding queue time and wait for finished)|
+------------------------+--------------------------------------------------------------------------+
|op_r_latency            | Latency of read operation (including queue time)                         |
+------------------------+--------------------------------------------------------------------------+
|op_r_process_latency    | Latency of read operation (excluding queue time)                         |
+------------------------+--------------------------------------------------------------------------+
|op_w_in_bytes           | Client data written                                                      |
+------------------------+--------------------------------------------------------------------------+
|op_w_latency            | Latency of write operation (including queue time)                        |
+------------------------+--------------------------------------------------------------------------+
|op_w_process_latency    | Latency of write operation (excluding queue time)                        |
+------------------------+--------------------------------------------------------------------------+
|op_w_prepare_latency    | Latency of write operations (excluding queue time and wait for finished) |
+------------------------+--------------------------------------------------------------------------+
|op_rw                   | Client read-modify-write operations                                      |
+------------------------+--------------------------------------------------------------------------+
|op_rw_in_bytes          | Client read-modify-write operations write in                             |
+------------------------+--------------------------------------------------------------------------+
|op_rw_out_bytes         | Client read-modify-write operations read out                             |
+------------------------+--------------------------------------------------------------------------+
|op_rw_latency           | Latency of read-modify-write operation (including queue time)            |
+------------------------+--------------------------------------------------------------------------+
|op_rw_process_latency   | Latency of read-modify-write operation (excluding queue time)            |
+------------------------+--------------------------------------------------------------------------+
|op_rw_prepare_latency   | Latency of read-modify-write operations (excluding queue time            |
|                        | and wait for finished)                                                   |
+------------------------+--------------------------------------------------------------------------+
|op_before_queue_op_lat  | Latency of IO before calling queue (before really queue into ShardedOpWq)|
|                        | op_before_dequeue_op_lat                                                 |
+------------------------+--------------------------------------------------------------------------+
|op_before_dequeue_op_lat| Latency of IO before calling dequeue_op(already dequeued and get PG lock)|
+------------------------+--------------------------------------------------------------------------+
