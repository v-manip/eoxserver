#!/bin/bash
#
#       /etc/rc.d/init.d/
#
#
#
#
#

# Source function library.
. /etc/init.d/functions


AUTHPATH="/home/santilland/v-manip/eoxserver/eoxserver/services/auth/ngeopdp/authServer.py"
PIDFILE="/var/run/authserver.pid"
LOGFILE="/var/log/authserver.log"

start() {
        echo "Starting authServer"
        python $AUTHPATH > $LOGFILE 2> $LOGFILE &
        echo $! > $PIDFILE
        
        return
}

stop() {
        echo "Shutting down authServer"

        kill `cat $PIDFILE`
        rm -f $PIDFILE

        return
}

case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    status)

        ;;
    restart)
        stop
        start
        ;;
    *)
        echo "Usage:  {start|stop|status|restart}"
        exit 1
        ;;
esac
exit $?