#!/usr/bin/env bash

CONSUL_VERSION="1.7.0"
TINYDB_VERSION="3.15.2"
IDNA_VERSION="2.8"
REQUESTS_VERSION="2.22.0"
CHARDET_VERSION="3.0.4"
PYTHON_CERTIFI_VERSION="2019.11.28"
URLLIB3_VERSION="1.25.8"
SIX_SERVION="1.14.0"
PYTHON_CONSUL_VERSION="1.1.0"

# Usage: copyFileForceX <source path> <destination path>
function copyFileForceX {
    cp -rf $1 $2
    chmod -r 755 $2
}

# Usage: copyFileForceX <source path> <destination path>
function copyFileForce {
    cp -rf $1 $2
    chmod -r 744 $2
}

function uninstallQemubackService {
    if [ `cat /etc/redhat-release | awk '{print $1}'` = "XCP-ng" ]; then
        echo "Uninstalling Qemuback service"

        rm -f "/usr/lib/systemd/system/qemuback.service"
        rm -f "/usr/bin/qemuback.py"
    fi
}

function installQemubackService {
    if [ `cat /etc/redhat-release | awk '{print $1}'` = "XCP-ng" ]; then
        echo "Installing Qemuback service"

        copyFileForceX "src/qemuback/qemuback.service" "/usr/lib/systemd/system/qemuback.service"
        copyFileForceX "src/qemuback/qemuback.py" "/usr/bin/qemuback.py"
    fi
}

function uninstallPythonConsul {
    echo "  Uninstalling python-consul"

    rm -rf "/lib/python2.7/site-packages/consul"
}

function installPythonConsul {
    echo "  Installing python-consul"

    cd /tmp
    curl --silent --remote-name https://github.com/cablehead/python-consul/archive/v${PYTHON_CONSUL_VERSION}.zip

    copyFileForceX "python-consul-${PYTHON_CONSUL_VERSION}/consul" "/lib/python2.7/site-packages"
    rm -rf "python-consul-${PYTHON_CONSUL_VERSION}"
}

function uninstallSix {
    echo "  Uninstalling six"

    rm -rf "/lib/python2.7/site-packages/six.py"
}

function installSix {
    echo "  Installing six"

    cd /tmp
    curl --silent --remote-name https://github.com/benjaminp/six/archive/${SIX_SERVION}.zip
    unzip ${SIX_SERVION}.zip
    copyFileForceX "${SIX_SERVION}/six.py" "/lib/python2.7/site-packages"
    rm -rf "${PYTHON_CONSUL_VERSION}"
}

function uninstallUrllib3 {
    echo "  Uninstalling urllib3"

    rm -rf "/lib/python2.7/site-packages/urllib3"
}

function installUrllib3 {
    echo "  Installing urllib3"

    cd /tmp
    curl --silent --remote-name https://github.com/urllib3/urllib3/archive/${URLLIB3_VERSION}.zip
    unzip ${URLLIB3_VERSION}.zip
    copyFileForceX "${URLLIB3_VERSION}/src/urllib3" "/lib/python2.7/site-packages"
    rm -rf "${URLLIB3_VERSION}"
}

function uninstallPythonCertifi {
    echo "  Uninstalling python-certifi"

    rm -rf "/lib/python2.7/site-packages/certifi"
}

function installPythonCertifi {
    echo "  Installing python-certifi"

    cd /tmp
    curl --silent --remote-name https://github.com/certifi/python-certifi/archive/${PYTHON_CERTIFI_VERSION}.zip
    unzip ${PYTHON_CERTIFI_VERSION}.zip
    copyFileForceX "${PYTHON_CERTIFI_VERSION}/certifi" "/lib/python2.7/site-packages"
    rm -rf "${PYTHON_CERTIFI_VERSION}"
}

function uninstallChardet {
    echo "  Uninstalling chardet"

    rm -rf "/lib/python2.7/site-packages/chardet"
}

function installChardet {
    echo "  Installing chardet"

    cd /tmp
    curl --silent --remote-name https://github.com/chardet/chardet/archive/${CHARDET_VERSION}.zip
    unzip ${CHARDET_VERSION}.zip
    copyFileForceX "${CHARDET_VERSION}/chardet" "/lib/python2.7/site-packages"
    rm -rf "${CHARDET_VERSION}"
}

function uninstallRequests {
    echo "  Uninstalling requests"

    rm -rf "/lib/python2.7/site-packages/requests"
}

function installRequests {
    echo "  Installing requests"

    cd /tmp
    curl --silent --remote-name https://github.com/psf/requests/archive/v${REQUESTS_VERSION}.zip
    unzip v${REQUESTS_VERSION}.zip
    copyFileForceX "${REQUESTS_VERSION}/requests" "/lib/python2.7/site-packages"
    rm -rf "${REQUESTS_VERSION}"
}

function uninstallIdna {
    echo "  Uninstalling idna"

    rm -rf "/lib/python2.7/site-packages/idna"
}

function installIdna {
    echo "  Installing idna"

    cd /tmp
    curl --silent --remote-name https://github.com/kjd/idna/archive/v${IDNA_VERSION}.zip
    unzip v${IDNA_VERSION}.zip
    copyFileForceX "v${IDNA_VERSION}/idna" "/lib/python2.7/site-packages"
    rm -rf "v${REQUESTS_VERSION}"
}

function uninstallTinyDB {
    echo "  Uninstalling TinyDB"

    rm -rf "/lib/python2.7/site-packages/tinydb"
}

function installTinyDB {
    echo "  Installing TinyDB"

    cd /tmp
    curl --silent --remote-name https://github.com/msiemens/tinydb/archive/v${TINYDB_VERSION}.zip
    unzip v${TINYDB_VERSION}.zip
    copyFileForceX "v${TINYDB_VERSION}/tinydb" "/lib/python2.7/site-packages"
    rm -rf "v${REQUESTS_VERSION}"
}

function uninstallConsul {
    echo "  Stopping Consul Service"

    systemctl stop consul
    systemctl disable consul
    #systemctl status consul

    echo "  Uninstalling Consul"

    rm -f "/etc/systemd/system/consul.service"
    rm -rf "/etc/consul.d"
    rm -f "/usr/local/bin/consul"
    rm -rf "/opt/consul"

    userdel consul
}

function installConsul {
    echo "  Installing Consul"

    cd /tmp
    curl --silent --remote-name https://releases.hashicorp.com/consul/${CONSUL_VERSION}/consul_${CONSUL_VERSION}_linux_amd64.zip
    unzip consul_${CONSUL_VERSION}_linux_amd64.zip
    chown root:root consul
    mv consul /usr/local/bin/
    consul -autocomplete-install

    useradd --system --home /etc/consul.d --shell /bin/false consul
    mkdir --parents /opt/consul
    chown --recursive consul:consul /opt/consul

    touch /etc/systemd/system/consul.service

    cat <<EOF > /etc/systemd/system/consul.service
[Unit]
Description="HashiCorp Consul - A service mesh solution"
Documentation=https://www.consul.io/
Requires=network-online.target
After=network-online.target
ConditionFileNotEmpty=/etc/consul.d/consul.hcl

[Service]
Type=notify
User=consul
Group=consul
ExecStart=/usr/local/bin/consul agent -config-dir=/etc/consul.d/
ExecReload=/usr/local/bin/consul reload
KillMode=process
Restart=on-failure
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

    mkdir --parents /etc/consul.d
    touch /etc/consul.d/consul.hcl
    chown --recursive consul:consul /etc/consul.d
    chmod 640 /etc/consul.d/consul.hcl

    #xe pif-param-list uuid=`xe pif-list management=true | grep "^uuid ( RO)" | awk -F: '{print $2}' | sed "s/ //g"` | grep "IP ( RO):" | awk -F: '{print $2}' | sed "s/ //g"
    cat <<EOF > /etc/consul.d/consul.hcl
datacenter = "dc1"
bind = "{{ GetInterfaceIP "xenbr0" }}"
data_dir = "/opt/consul"
encrypt = "`/usr/local/bin/consul keygen`"

performance {
  raft_multiplier = 1
}
EOF

    mkdir --parents /etc/consul.d
    touch /etc/consul.d/server.hcl
    chown --recursive consul:consul /etc/consul.d
    chmod 640 /etc/consul.d/server.hcl

    cat <<EOF > /etc/consul.d/server.hcl
server = true
ui = true
EOF

    echo "  Starting Consul Service"
    systemctl enable consul
    systemctl start consul
    #systemctl status consul
}

function uninstallDependencies {
    echo "Installing dependencies"

    uninstallTinyDB
    uninstallConsul
    uninstallPythonConsul
    uninstallSix
    uninstallChardet
    uninstallIdna
    uninstallPythonCertifi
    uninstallRequests
    uninstallUrllib3
}

function installDependencies {
    echo "Installing dependencies"

    installSix
    installChardet
    installIdna
    installPythonCertifi
    installRequests
    installUrllib3
    installTinyDB
    installPythonConsul
    installConsul
}

function configureFirewall {
    #iptables -A INPUT -p tcp --dport 6789 -j ACCEPT
    #iptables -A INPUT -m multiport -p tcp --dports 6800:7300 -j ACCEPT
    #service iptables save
    :
}

function unconfigureFirewall {
    #iptables -D INPUT -p tcp --dport 6789 -j ACCEPT
    #iptables -D INPUT -m multiport -p tcp --dports 6800:7300 -j ACCEPT
    #service iptables save
    :
}

function uninstallFiles {
    echo "Uninstalling xcpng-storage-libs"

    rm -rf "/lib/python2.7/site-packages/xapi/storage/libs/xcpng"
    rm -rf "/usr/libexec/xapi/cluster-stack/consul"
}


function installFiles {
    echo "Installing xcpng-storage-libs"

    mkdir -p /lib/python2.7/site-packages/xapi/storage/libs/xcpng/cluster_stack
    copyFileForceX "src/xapi/storage/libs/xcpng/cluster_stack/__init__.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/cluster_stack/__init__.py"
    copyFileForceX "src/xapi/storage/libs/xcpng/cluster_stack/consul/__init__.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/cluster_stack/consul/__init__.py"
    copyFileForceX "src/xapi/storage/libs/xcpng/cluster_stack/consul/ha.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/cluster_stack/consul/ha.py"
    copyFileForceX "src/xapi/storage/libs/xcpng/cluster_stack/consul/locks.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/cluster_stack/consul/locks.py"
    copyFileForceX "src/xapi/storage/libs/xcpng/cluster_stack/consul/tinydb_storage.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/cluster_stack/consul/tinydb_storage.py"

    mkdir -p /usr/libexec/xapi/cluster-stack/consul
    copyFileForceX "src/xapi/storage/libs/xcpng/cluster_stack/consul/ha_clear_excluded" "/usr/libexec/xapi/cluster-stack/consul/ha_clear_excluded"
    copyFileForceX "src/xapi/storage/libs/xcpng/cluster_stack/consul/ha_master_lock" "/usr/libexec/xapi/cluster-stack/consul/ha_master_lock"
    copyFileForceX "src/xapi/storage/libs/xcpng/cluster_stack/consul/ha_propose_master" "/usr/libexec/xapi/cluster-stack/consul/ha_propose_master"
    copyFileForceX "src/xapi/storage/libs/xcpng/cluster_stack/consul/ha_query_liveset" "/usr/libexec/xapi/cluster-stack/consul/ha_query_liveset"
    copyFileForceX "src/xapi/storage/libs/xcpng/cluster_stack/consul/ha_set_excluded" "/usr/libexec/xapi/cluster-stack/consul/ha_set_excluded"
    copyFileForceX "src/xapi/storage/libs/xcpng/cluster_stack/consul/ha_set_pool_state" "/usr/libexec/xapi/cluster-stack/consul/ha_set_pool_state"
    copyFileForceX "src/xapi/storage/libs/xcpng/cluster_stack/consul/ha_start_daemon" "/usr/libexec/xapi/cluster-stack/consul/ha_start_daemon"
    copyFileForceX "src/xapi/storage/libs/xcpng/cluster_stack/consul/ha_stop_daemon" "/usr/libexec/xapi/cluster-stack/consul/ha_stop_daemon"
    copyFileForceX "src/xapi/storage/libs/xcpng/cluster_stack/consul/ha_suppoted_srs" "/usr/libexec/xapi/cluster-stack/consul/ha_supported_srs"

    mkdir -p /lib/python2.7/site-packages/xapi/storage/libs/xcpng/scripts
    copyFileForceX "src/xapi/storage/libs/xcpng/scripts/__init__.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/scripts/__init__.py"
    copyFileForceX "src/xapi/storage/libs/xcpng/scripts/data.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/scripts/data.py"
    copyFileForceX "src/xapi/storage/libs/xcpng/scripts/datapath.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/scripts/datapath.py"
    copyFileForceX "src/xapi/storage/libs/xcpng/scripts/plugin.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/scripts/plugin.py"
    copyFileForceX "src/xapi/storage/libs/xcpng/scripts/sr.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/scripts/sr.py"
    copyFileForceX "src/xapi/storage/libs/xcpng/scripts/volume.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/scripts/volume.py"

    copyFileForceX "src/xapi/storage/libs/xcpng/__init__.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/__init__.py"
    copyFileForceX "src/xapi/storage/libs/xcpng/data.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/data.py"
    copyFileForceX "src/xapi/storage/libs/xcpng/datapath.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/datapath.py"
    copyFileForceX "src/xapi/storage/libs/xcpng/gc.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/gc.py"
    copyFileForceX "src/xapi/storage/libs/xcpng/globalvars.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/globalvars.py"
    copyFileForceX "src/xapi/storage/libs/xcpng/meta.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/meta.py"
    copyFileForceX "src/xapi/storage/libs/xcpng/qemudisk.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/qemudisk.py"
    copyFileForceX "src/xapi/storage/libs/xcpng/sr.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/sr.py"
    copyFileForceX "src/xapi/storage/libs/xcpng/tapdisk.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/tapdisk.py"
    copyFileForceX "src/xapi/storage/libs/xcpng/utils.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/utils.py"
    copyFileForceX "src/xapi/storage/libs/xcpng/volume.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/volume.py"

    ln -s "/usr/share/qemu/qmp/qmp.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/qmp.py"
}

function install {
    installDependencies
    installFiles
    configureFirewall
    installQemubackService
}

function uninstall {
    unconfigureFirewall
    uninstallFiles
    uninstallDependencies
    uninstallQemubackService
}

# Usage: confirmInstallation
function confirmInstallation {
  echo "This script is going to install 'xcpng-storage-libs' package and its dependencises"
  echo "Please note that 'xcpng-storage-libs' package is experimental and can lead to"
  echo "an unstable system and data loss"
  default='no'
  while true; do
    read -p "Continue? (y[es]/n[o]) [${default}]: " yesorno
    if [ -z ${yesorno} ];then
      yesorno=${default}
    fi
    case ${yesorno} in
      yes|y)
            ret=0
            break
            ;;
       no|n)
            ret=1
            break
            ;;
    esac
  done
  return ${ret}
}

case $1 in
    install)
        if confirmInstallation; then
            install
        fi
        ;;
    uninstall)
        uninstall
        ;;
    *)
        echo "Usage: $0 install|uninstall"
        exit 1
        ;;
esac