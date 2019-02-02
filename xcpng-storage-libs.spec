Summary: xcp-storage-libs - Base classes for XCP-ng Storage Manager (smapiv3) plugins
Name: xcp-storage-libs
Epoch: 1
Version: 1.0
Release: 1
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
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/meta.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/meta.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/qemudisk.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/qemudisk.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/sr.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/sr.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/tapdisk.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/tapdisk.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/utils.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/utils.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/xcpng/volume.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/xcpng/volume.py
#---

%files
/lib/python2.7/site-packages/xapi/storage/libs/xcpng


%changelog
* Sun Jan 27 2019 rposudnevskiy <ramzes_r@yahoo.com> - 1.0-1
- First packaging
