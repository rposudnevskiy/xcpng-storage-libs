Summary: xcp-storage-libs - Base classes for XCP-ng Storage Manager (smapiv3) plugins
Name: xcp-storage-libs
Epoch: 1
Version: 1.0
Release: 2
License: LGPL
Group: Utilities/System
BuildArch: noarch
URL: https://github.com/rposudnevskiy/%{name}
Requires: python-rbd
Requires: rbd-nbd
Requires: qemu
Requires: qemu-dp
Requires: glibc >= 2.17-222.el7
%undefine _disable_source_fetch
Source0: https://github.com/rposudnevskiy/%{name}/archive/v%{version}.zip

%description
This package contains xcp-storage-libs - Base classes for XCP-ng Storage Manager (smapiv3) plugins


%prep
%autosetup


%install
rm -rf %{builddir}
rm -rf %{buildroot}
#---
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/__init__.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/__init__.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/data.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/data.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/datapath.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/datapath.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/gc.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/gc.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/globalvars.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/globalvars.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/meta.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/meta.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/qemudisk.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/qemudisk.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/sr.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/sr.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/tapdisk.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/tapdisk.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/utils.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/utils.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/volume.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/volume.py
#---
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/scripts/__init__.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/scripts/__init__.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/scripts/data.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/scripts/data.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/scripts/datapath.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/scripts/datapath.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/scripts/plugin.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/scripts/plugin.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/scripts/sr.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/scripts/sr.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/scripts/volume.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/scripts/volume.py
#---
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/cluster_stack/__init__.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/cluster_stack/__init__.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/cluster_stack/consul/__init__.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/cluster_stack/consul/__init__.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/cluster_stack/consul/ha.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/cluster_stack/consul/ha.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/cluster_stack/consul/ha_clear_excluded %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/cluster_stack/consul/ha_clear_excluded
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/cluster_stack/consul/__init__.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/cluster_stack/consul/ha_disarm_fencing
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/cluster_stack/consul/ha_master_lock %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/cluster_stack/consul/ha_master_lock
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/cluster_stack/consul/ha_propose_master %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/cluster_stack/consul/ha_propose_master
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/cluster_stack/consul/ha_query_liveset %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/cluster_stack/consul/ha_query_liveset
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/cluster_stack/consul/ha_set_excluded %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/cluster_stack/consul/ha_set_excluded
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/cluster_stack/consul/ha_set_pool_state %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/cluster_stack/consul/ha_set_pool_state
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/cluster_stack/consul/ha_start_daemon %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/cluster_stack/consul/ha_start_daemon
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/cluster_stack/consul/ha_stop_daemon %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/cluster_stack/consul/ha_stop_daemon
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/cluster_stack/consul/ha_suppoted_srs %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/cluster_stack/consul/ha_supported_srs
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/cluster_stack/consul/locks.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/cluster_stack/consul/locks.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/cluster_stack/consul/tinydb_storage.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/cluster_stack/consul/tinydb_storage.py

%files
/lib/python2.7/site-packages/xapi/storage/libs/xcpng


%changelog
* Sun Jan 27 2019 rposudnevskiy <ramzes_r@yahoo.com> - 1.0-1
- First packaging
