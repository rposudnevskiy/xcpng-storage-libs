#!/bin/bash
if [[ `cat /etc/xensource/pool.conf` = 'master' ]]; then
        /usr/local/bin/consul agent -config-dir=/etc/consul.d/ -server -bootstrap
else
        if [[ `consul members -http-addr="http://192.168.57.200:8500" | grep server | wc -l` -le 1 ]]; then
                /usr/local/bin/consul agent -config-dir=/etc/consul.d/ -server -retry-join \
                    \"`xe host-param-get param-name=address uuid=\`xe pool-list | grep master | awk '{print $4}'\``\"
        else
                /usr/local/bin/consul agent -config-dir=/etc/consul.d/ -retry-join \
                    \"`xe host-param-get param-name=address uuid=\`xe pool-list | grep master | awk '{print $4}'\``\"
        fi
fi