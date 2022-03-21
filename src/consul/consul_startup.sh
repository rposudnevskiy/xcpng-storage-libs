#!/bin/bash
if [[ `cat /etc/xensource/pool.conf` = 'master' ]]; then
        /usr/local/bin/consul agent -config-dir=/etc/consul.d/ -server -bootstrap
else
        pool_master=`awk -F: '{print $2}' /etc/xensource/pool.conf`
        if [[ `consul members -http-addr="http://$pool_master:8500" | grep server | wc -l` -le 5 ]]; then
                /usr/local/bin/consul agent -config-dir=/etc/consul.d/ -server -retry-join "$pool_master"
        else
                /usr/local/bin/consul agent -config-dir=/etc/consul.d/ -retry-join "$pool_master"
        fi
fi