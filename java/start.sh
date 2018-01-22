wccConfDir="$rootDir/conf/sec"                      # directory of the wcc config

applDir="$rootDir/lib"                                   # home directory of the service application
serviceName="secd"                                         # service name
maxShutdownTime=15                                         # maximum number of seconds to wait for the daemon to terminate normally
LOGFILE="/var/log/dfv/oam/oam-u/secd/script.log"                      # log file for StdOut/StdErr
pidFile="${OMS_TMP_PATH}/${serviceName}.pid"                            # name of PID file (PID = process ID number)
LOCKFILE="${OMS_TMP_PATH}/.${serviceName}.lock"

className="com.omm.secs.service.StartService"
javaCommand="$JRE_HOME/bin/java"                                         # name of the Java launcher without the path
jvmbase="-Xms16m -Xmx32m -XX:PermSize=32m -XX:MaxPermSize=32m"
classpath=""
jar_list="netty*.jar jboss-serialization*.jar oam_framework*.jar jackson-databind*.jar jackson-core*.jar jackson-annotations*.jar wc2frm*.jar gsjdbc*.jar jaxen*.jar apache-log4j-extras-*.jar commons-collections-*.jar commons-codec-*.jar commons-lang-*.jar commons-logging-*.jar dom4j-*.jar ehcache*.jar fluent-hc-*.jar gm_adapter-*.jar httpclient-*.jar httpcore-*.jar jackson-core-asl-*.jar jackson-mapper-asl-*.jar jackson-xc-*.jar jta-*.jar libfb303-*.jar libthrift-*.jar log4j-*.jar SECS-*.jar servlet-api-*.jar slf4j-api-*.jar trove*.jar xml-apis-*.jar xmlpull-*.jar xpp3_min-*.jar xstream-*.jar"

#动态加载jar包
for f in $jar_list; do
    jar=$(find ${applDir} -name $f | head -1)
    [ -n "$jar" ] && classpath=${classpath}:$jar;
done

# arguments for Java launcher
javaArgs="-Dprocess.name=${serviceName} ${jvmbase} -Dbase.dir=${applDir} -cp ${classpath} -Dbeetle.application.home.path=${wccConfDir} -Dnet.sf.ehcache.skipUpdateCheck=true ${className}"
javaCommandLine="$javaCommand $javaArgs"                   # command line to start the Java service application
javaCommandLineKeyword="${serviceName}"                    # used to detect an already running service process and to distinguish it from others
