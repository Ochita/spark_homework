import os

os.environ['JAVA_HOME'] = os.environ.get('JAVA_HOME') or '/usr/local/opt/openjdk@8'
os.environ['JAVA_OPTS'] = '-Dio.netty.tryReflectionSetAccessible=true'

