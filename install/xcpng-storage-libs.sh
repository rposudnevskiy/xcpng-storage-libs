#!/usr/bin/env bash

CONSUL_VERSION="1.11.4"
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
    chmod -R 755 $2
}

# Usage: copyFileForceX <source path> <destination path>
function copyFileForce {
    cp -rf $1 $2
    chmod -R 744 $2
}

function installNbd {
    echo "  Installing NBD Client"
    yum install --enablerepo="base,extras,epel" -q -y nbd
}

function uninstallNbd {
    echo "  Uninstall NBD Client"
    yum erase -q -y nbd
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

function installQemudp {
    echo "  Installing Qemu-dp"

    wget -q -nd -P /tmp -r -R 'index.html*' -e robots=off \
      --accept-regex 'qemu-dp-xcpng-[[:digit:].]*-[[:digit:].]*.x86_64.rpm' \
      https://github.com/rposudnevskiy/qemu-dp/releases/tag/xcpng-testing-`cut -d" " -f3 /etc/redhat-release | cut -c1-3`/
    yum install --enablerepo="base,extras,epel" -q -y /tmp/qemu-dp-xcpng-*.rpm
    rm -f /tmp/qemu-dp-xcpng-*.rpm
}

function uninstallQemudp {
    echo "  Uninstalling Qemu-dp"
    yum erase -y qemu-dp
}

function uninstallPythonConsul {
    echo "  Uninstalling python-consul"

    rm -rf "/lib/python2.7/site-packages/consul"
}

function installPythonConsul {
    echo "  Installing python-consul"

    wget -q https://github.com/cablehead/python-consul/archive/v${PYTHON_CONSUL_VERSION}.zip -O /tmp/v${PYTHON_CONSUL_VERSION}.zip
    unzip -qq /tmp/v${PYTHON_CONSUL_VERSION}.zip -d /tmp
    copyFileForceX "/tmp/python-consul-${PYTHON_CONSUL_VERSION}/consul" "/lib/python2.7/site-packages/consul"
    rm -rf "/tmp/python-consul-${PYTHON_CONSUL_VERSION}"
    rm -f /tmp/v${PYTHON_CONSUL_VERSION}.zip
}

function uninstallSix {
    echo "  Uninstalling six"

    rm -rf "/lib/python2.7/site-packages/six.py"
}

function installSix {
    echo "  Installing six"

    wget -q https://github.com/benjaminp/six/archive/${SIX_SERVION}.zip -O /tmp/${SIX_SERVION}.zip
    unzip -qq /tmp/${SIX_SERVION}.zip -d /tmp
    copyFileForceX "/tmp/six-${SIX_SERVION}/six.py" "/lib/python2.7/site-packages/six.py"
    rm -rf "/tmp/six-${SIX_SERVION}"
    rm -f /tmp/${SIX_SERVION}.zip
}

function uninstallUrllib3 {
    echo "  Uninstalling urllib3"

    rm -rf "/lib/python2.7/site-packages/urllib3"
}

function installUrllib3 {
    echo "  Installing urllib3"

    wget -q https://github.com/urllib3/urllib3/archive/${URLLIB3_VERSION}.zip -O /tmp/${URLLIB3_VERSION}.zip
    unzip -qq /tmp/${URLLIB3_VERSION}.zip -d /tmp
    copyFileForceX "/tmp/urllib3-${URLLIB3_VERSION}/src/urllib3" "/lib/python2.7/site-packages/urllib3"
    rm -rf "/tmp/urllib3-${URLLIB3_VERSION}"
    rm -f /tmp/${URLLIB3_VERSION}.zip
}

function uninstallPythonCertifi {
    echo "  Uninstalling python-certifi"

    rm -rf "/lib/python2.7/site-packages/certifi"
}

function installPythonCertifi {
    echo "  Installing python-certifi"

    wget -q https://github.com/certifi/python-certifi/archive/${PYTHON_CERTIFI_VERSION}.zip -O /tmp/${PYTHON_CERTIFI_VERSION}.zip
    unzip -qq /tmp/${PYTHON_CERTIFI_VERSION}.zip -d /tmp
    copyFileForceX "/tmp/python-certifi-${PYTHON_CERTIFI_VERSION}/certifi" "/lib/python2.7/site-packages/certifi"
    rm -rf "/tmp/python-certifi-${PYTHON_CERTIFI_VERSION}"
    rm -f /tmp/${PYTHON_CERTIFI_VERSION}.zip
}

function uninstallChardet {
    echo "  Uninstalling chardet"

    rm -rf "/lib/python2.7/site-packages/chardet"
}

function installChardet {
    echo "  Installing chardet"

    wget -q https://github.com/chardet/chardet/archive/${CHARDET_VERSION}.zip -O /tmp/${CHARDET_VERSION}.zip
    unzip -qq /tmp/${CHARDET_VERSION}.zip -d /tmp
    copyFileForceX "/tmp/chardet-${CHARDET_VERSION}/chardet" "/lib/python2.7/site-packages/chardet"
    rm -rf "/tmp/chardet-${CHARDET_VERSION}"
    rm -f /tmp/${CHARDET_VERSION}.zip
}

function uninstallRequests {
    echo "  Uninstalling requests"

    rm -rf "/lib/python2.7/site-packages/requests"
}

function installRequests {
    echo "  Installing requests"

    wget -q https://github.com/psf/requests/archive/v${REQUESTS_VERSION}.zip -O /tmp/v${REQUESTS_VERSION}.zip
    unzip -qq /tmp/v${REQUESTS_VERSION}.zip -d /tmp
    copyFileForceX "/tmp/requests-${REQUESTS_VERSION}/requests" "/lib/python2.7/site-packages/requests"
    rm -rf "/tmp/requests-${REQUESTS_VERSION}"
    rm -f /tmp/v${REQUESTS_VERSION}.zip
}

function uninstallIdna {
    echo "  Uninstalling idna"

    rm -rf "/lib/python2.7/site-packages/idna"
}

function installIdna {
    echo "  Installing idna"

    wget -q https://github.com/kjd/idna/archive/v${IDNA_VERSION}.zip -O /tmp/v${IDNA_VERSION}.zip
    unzip -qq /tmp/v${IDNA_VERSION}.zip -d /tmp
    copyFileForceX "/tmp/idna-${IDNA_VERSION}/idna" "/lib/python2.7/site-packages/idna"
    rm -rf "/tmp/idna-${IDNA_VERSION}"
    rm -f /tmp/v${IDNA_VERSION}.zip
}

function uninstallTinyDB {
    echo "  Uninstalling TinyDB"

    rm -rf "/lib/python2.7/site-packages/tinydb"
}

function installTinyDB {
    echo "  Installing TinyDB"

    wget -q https://github.com/msiemens/tinydb/archive/v${TINYDB_VERSION}.zip -O /tmp/v${TINYDB_VERSION}.zip
    unzip -qq /tmp/v${TINYDB_VERSION}.zip -d /tmp
    copyFileForceX "/tmp/tinydb-${TINYDB_VERSION}/tinydb" "/lib/python2.7/site-packages/tinydb"
    rm -rf "/tmp/tinydb-${TINYDB_VERSION}"
    rm -f /tmp/v${TINYDB_VERSION}.zip
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
    rm -f "/usr/local/bin/consul_startup.sh"
    rm -rf "/opt/consul"

    userdel consul
}

function installConsul {
    echo "  Installing Consul"

    wget -q https://releases.hashicorp.com/consul/${CONSUL_VERSION}/consul_${CONSUL_VERSION}_linux_amd64.zip -O /tmp/consul_${CONSUL_VERSION}_linux_amd64.zip
    unzip -qq /tmp/consul_${CONSUL_VERSION}_linux_amd64.zip -d /tmp
    rm -f /tmp/consul_${CONSUL_VERSION}_linux_amd64.zip
    chown root:root /tmp/consul
    mv /tmp/consul /usr/local/bin/
    consul -autocomplete-install >/dev/null 2>&1

    id -u consul >/dev/null 2>&1 || useradd --system --home /etc/consul.d --shell /bin/false consul
    mkdir --parents /opt/consul
    chown --recursive consul:consul /opt/consul

    copyFileForce "src/consul/consul.service" "/etc/systemd/system/consul.service"

    mkdir --parents /etc/consul.d
    copyFileForce "src/consul/consul.hcl" "/etc/consul.d/consul.hcl"
    chown --recursive consul:consul /etc/consul.d
    chmod 640 /etc/consul.d/consul.hcl

    copyFileForce "src/consul/consul_startup.sh" "/usr/local/bin/consul_startup.sh"
    chmod 640 /usr/local/bin/consul_startup.sh

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
    uninstallNbd
    uninstallQemudp
    uninstallQemubackService
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
    installNbd
    installConsul
    installQemudp
    installQemubackService
}

function configureFirewall {
    #iptables -A INPUT -p tcp --dport 6789 -j ACCEPT
    #iptables -A INPUT -m multiport -p tcp --dports 6800:7300 -j ACCEPT

    # Configure Consul ports
    iptables -A INPUT -p tcp --dport 8300 -j ACCEPT
    iptables -A INPUT -p tcp --dport 8301 -j ACCEPT
    iptables -A INPUT -p tcp --dport 8302 -j ACCEPT
    iptables -A INPUT -p tcp --dport 8500 -j ACCEPT
    iptables -A INPUT -p tcp --dport 8600 -j ACCEPT

    service iptables save
    #:
}

function unconfigureFirewall {
    #iptables -D INPUT -p tcp --dport 6789 -j ACCEPT
    #iptables -D INPUT -m multiport -p tcp --dports 6800:7300 -j ACCEPT

    # Configure Consul ports
    iptables -D INPUT -p tcp --dport 8300 -j ACCEPT
    iptables -D INPUT -p tcp --dport 8301 -j ACCEPT
    iptables -D INPUT -p tcp --dport 8302 -j ACCEPT
    iptables -D INPUT -p tcp --dport 8500 -j ACCEPT
    iptables -D INPUT -p tcp --dport 8600 -j ACCEPT

    service iptables save
    #:
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
    mkdir -p /lib/python2.7/site-packages/xapi/storage/libs/xcpng/cluster_stack/consul
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
    copyFileForceX "src/xapi/storage/libs/xcpng/cluster_stack/consul/ha_supported_srs" "/usr/libexec/xapi/cluster-stack/consul/ha_supported_srs"

    mkdir -p /lib/python2.7/site-packages/xapi/storage/libs/xcpng/scripts
    copyFileForceX "src/xapi/storage/libs/xcpng/scripts/__init__.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/scripts/__init__.py"
    copyFileForceX "src/xapi/storage/libs/xcpng/scripts/data.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/scripts/data.py"
    copyFileForceX "src/xapi/storage/libs/xcpng/scripts/datapath.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/scripts/datapath.py"
    copyFileForceX "src/xapi/storage/libs/xcpng/scripts/plugin.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/scripts/plugin.py"
    copyFileForceX "src/xapi/storage/libs/xcpng/scripts/sr.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/scripts/sr.py"
    copyFileForceX "src/xapi/storage/libs/xcpng/scripts/volume.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/scripts/volume.py"

    copyFileForceX "src/xapi/storage/libs/__init__.py" "/lib/python2.7/site-packages/xapi/storage/libs/__init__.py"
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

    if [[ -x /usr/share/qemu/qmp/qmp ]]; then
      ln -f -s "/usr/share/qemu/qmp/qmp" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/qmp.py"
    elif [[ -x /usr/share/qemu/qmp/qmp.py ]]; then
      ln -f -s "/usr/share/qemu/qmp/qmp.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/qmp.py"
    fi
}

function install {
    installDependencies
    installFiles
    configureFirewall
}

function uninstall {
    unconfigureFirewall
    uninstallFiles
    uninstallDependencies
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