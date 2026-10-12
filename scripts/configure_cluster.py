"""Generate an isolated, loopback-only teaching cluster configuration."""
from pathlib import Path
import xml.etree.ElementTree as ET

root = Path(__file__).resolve().parents[1]
(root / 'conf').mkdir(parents=True, exist_ok=True)
runtime = root / 'runtime'
for d in ['namenode', 'datanode', 'logs', 'pids', 'yarn-local', 'yarn-logs']:
    (runtime / d).mkdir(parents=True, exist_ok=True)
settings = {
 'capacity-scheduler.xml': {'yarn.scheduler.capacity.root.queues': 'default',
   'yarn.scheduler.capacity.root.default.capacity': '100',
   'yarn.scheduler.capacity.root.default.maximum-capacity': '100',
   'yarn.scheduler.capacity.root.default.state': 'RUNNING',
   'yarn.scheduler.capacity.root.default.acl_submit_applications': '*',
   'yarn.scheduler.capacity.maximum-am-resource-percent': '0.5'},
 'core-site.xml': {'fs.defaultFS': 'hdfs://localhost:19000', 'hadoop.tmp.dir': str(runtime / 'tmp')},
 'hdfs-site.xml': {
   'dfs.replication': '1', 'dfs.namenode.name.dir': f'file://{runtime}/namenode',
   'dfs.datanode.data.dir': f'file://{runtime}/datanode',
   'dfs.namenode.rpc-address': 'localhost:19000', 'dfs.namenode.http-address': '127.0.0.1:19870',
   'dfs.datanode.address': '127.0.0.1:19866', 'dfs.datanode.http.address': '127.0.0.1:19864',
   'dfs.datanode.ipc.address': '127.0.0.1:19867', 'dfs.permissions.enabled': 'true'},
 'yarn-site.xml': {
   'yarn.resourcemanager.hostname': 'localhost', 'yarn.resourcemanager.bind-host': '127.0.0.1',
   'yarn.resourcemanager.address': 'localhost:18032', 'yarn.resourcemanager.scheduler.address': 'localhost:18030',
   'yarn.resourcemanager.resource-tracker.address': 'localhost:18031',
   'yarn.resourcemanager.admin.address': 'localhost:18033', 'yarn.resourcemanager.webapp.address': '127.0.0.1:18088',
   'yarn.nodemanager.hostname': 'localhost', 'yarn.nodemanager.bind-host': '127.0.0.1',
   'yarn.nodemanager.address': 'localhost:18041', 'yarn.nodemanager.localizer.address': 'localhost:18040',
   'yarn.nodemanager.webapp.address': '127.0.0.1:18042',
   'yarn.nodemanager.aux-services': 'mapreduce_shuffle',
   'yarn.nodemanager.local-dirs': str(runtime / 'yarn-local'), 'yarn.nodemanager.log-dirs': str(runtime / 'yarn-logs'),
   'yarn.nodemanager.resource.memory-mb': '4096', 'yarn.nodemanager.resource.cpu-vcores': '4',
   'yarn.scheduler.minimum-allocation-mb': '256', 'yarn.scheduler.maximum-allocation-mb': '4096',
   'yarn.nodemanager.vmem-check-enabled': 'false', 'yarn.nodemanager.pmem-check-enabled': 'false',
   'yarn.nodemanager.env-whitelist': 'JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME'},
 'mapred-site.xml': {'mapreduce.framework.name': 'yarn',
   'mapreduce.application.classpath': '/usr/local/hadoop/share/hadoop/mapreduce/*,/usr/local/hadoop/share/hadoop/mapreduce/lib/*',
   'yarn.app.mapreduce.am.env': 'HADOOP_MAPRED_HOME=/usr/local/hadoop',
   'mapreduce.map.env': 'HADOOP_MAPRED_HOME=/usr/local/hadoop',
   'mapreduce.reduce.env': 'HADOOP_MAPRED_HOME=/usr/local/hadoop',
   'mapreduce.map.memory.mb': '512', 'mapreduce.reduce.memory.mb': '512',
   'yarn.app.mapreduce.am.resource.mb': '512'}
}
for name, props in settings.items():
    tree = ET.Element('configuration')
    for key, value in props.items():
        p = ET.SubElement(tree, 'property')
        ET.SubElement(p, 'name').text = key
        ET.SubElement(p, 'value').text = value
    ET.indent(tree)
    ET.ElementTree(tree).write(root / 'conf' / name, encoding='utf-8', xml_declaration=True)
print(f'Cluster configuration: {root / "conf"}')
(root / 'conf/log4j.properties').write_text('''log4j.rootLogger=INFO,console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d %-5p %c: %m%n
''')
