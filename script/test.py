import settings
import os
import sys

def run():
    projroot = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'promviz')
    f = os.popen('/usr/lib/jvm/java-7-openjdk-amd64/bin/java -ea -Xms%(memory)s -Xloggc:/tmp/gc -Dfile.encoding=UTF-8 -classpath %(root)s/bin:%(root)s/lib/guava-14.0.1.jar:%(root)s/lib/gson-2.2.4.jar promviz.Main %(args)s' % {'args': ' '.join("'%s'" % k for k in sys.argv[1:]), 'memory': settings.memory, 'root': projroot})

def init_dirs():
    for d in [getattr(settings, k) for k in dir(settings) if k.startswith('dir_')]:
        os.popen('mkdir -p "%s"' % d)

if __name__ == "__main__":

    init_dirs()
    run()
