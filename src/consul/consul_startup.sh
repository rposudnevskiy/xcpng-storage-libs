#!/bin/bash
if [[ `cat /etc/xensource/pool.conf` = 'master' ]]; then
        /usr/local/bin/consul agent -config-dir=/etc/consul.d/ -server -bootstrap
else
        pool_master=`xe host-param-get param-name=address uuid=\`xe pool-list | grep master | awk '{print $4}'\``
        if [[ `consul members -http-addr="http://$pool_master:8500" | grep server | wc -l` -le 1 ]]; then
                /usr/local/bin/consul agent -config-dir=/etc/consul.d/ -server -retry-join "$pool_master"
        else
                /usr/local/bin/consul agent -config-dir=/etc/consul.d/ -retry-join "$pool_master"
        fi
fi