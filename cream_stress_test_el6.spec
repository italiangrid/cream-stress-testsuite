Summary: Stress testing suite and graph plot scripts for CREAM
Name: cream_stress_test
Version: 1.0.el6
Release: 1
Source0: cream_stress_test-1.0.el6.tar.gz
License: GPLv3
Group: GroupName
BuildArch: noarch
BuildRoot: %{_tmppath}/%{name}-buildroot
Requires: pexpect python-argparse python-matplotlib
%description
This is a CREAM stress test suite, written in python.
Documentation is also provided with this package.
Packaged for SL6 versions.
%prep
%setup -q
%build
%install
install -m 0755 -d $RPM_BUILD_ROOT/opt/cream_stress_test
install -m 0755 -d $RPM_BUILD_ROOT/opt/cream_stress_test/docs
install -m 0755 -d $RPM_BUILD_ROOT/opt/cream_stress_test/bin
install -m 0755 cream_stress_statistics.py $RPM_BUILD_ROOT/opt/cream_stress_test/bin/cream_stress_statistics.py
install -m 0755 cream_stress_test.py $RPM_BUILD_ROOT/opt/cream_stress_test/bin/cream_stress_test.py
install -m 0755 cream_stress_test.7.gz $RPM_BUILD_ROOT/opt/cream_stress_test/docs/cream_stress_test.7.gz
install -m 0755 cream_stress_statistics.7.gz $RPM_BUILD_ROOT/opt/cream_stress_test/docs/cream_stress_statistics.7.gz
install -m 0755 COPYING $RPM_BUILD_ROOT/opt/cream_stress_test/docs/COPYING
install -m 0755 CHANGELOG $RPM_BUILD_ROOT/opt/cream_stress_test/docs/CHANGELOG
%clean
rm -rf $RPM_BUILD_ROOT
%post
cp $RPM_BUILD_ROOT/opt/cream_stress_test/docs/cream_stress_test.7.gz /usr/share/man/man7/cream_stress_test.7.gz
cp $RPM_BUILD_ROOT/opt/cream_stress_test/docs/cream_stress_statistics.7.gz /usr/share/man/man7/cream_stress_statistics.7.gz
echo " "
echo "Package cream_stress_test installed succesfully!"
%files
%dir /opt/cream_stress_test
%dir /opt/cream_stress_test/bin
%dir /opt/cream_stress_test/docs
/opt/cream_stress_test/bin/cream_stress_statistics.py
/opt/cream_stress_test/bin/cream_stress_test.py
/opt/cream_stress_test/docs/cream_stress_statistics.7.gz
/opt/cream_stress_test/docs/cream_stress_test.7.gz
/opt/cream_stress_test/docs/COPYING
/opt/cream_stress_test/docs/CHANGELOG
