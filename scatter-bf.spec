Summary:	scatter - distributed messaging framework
Name:		scatter
Version:	0.1.1
Release:	1%{?dist}

License:	GPL 2+
Group: 		Development/Libraries
URL:		https://github.com/bioothod/scatter
Source0:	%{name}-%{version}.tar.bz2
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires:	boost-devel, boost-system, boost-thread, boost-program-options
BuildRequires:  cmake, msgpack-devel, gtest-devel, glog-devel

%global debug_package %{nil}

%description
Scatter allows to easily write p2p, client-server and server-server messaging systems.
Servers are organized into DHT-like ring with dynamic route table, new servers take load
from older ones, clients automatically detect new servers and reconnect.
Client joins broadcast groups on servers similar to how multicast groups are formed.
Servers are stateless, they only maintain global route table and forward requests to
servers which have to handle given request according to current route table snapshot.

%package devel
Summary:	scatter - distributed messaging framework, development files
Provides:	scatter-static = %{version}-%{release}

%description devel
Scatter allows to easily write p2p, client-server and server-server messaging systems.
Servers are organized into DHT-like ring with dynamic route table, new servers take load
from older ones, clients automatically detect new servers and reconnect.
Client joins broadcast groups on servers similar to how multicast groups are formed.
Servers are stateless, they only maintain global route table and forward requests to
servers which have to handle given request according to current route table snapshot.

%prep
%setup -q

%build
%{cmake} .

make %{?_smp_mflags}
#make test

%install
rm -rf %{buildroot}
make install DESTDIR=%{buildroot}

%post -p /sbin/ldconfig
%postun -p /sbin/ldconfig


%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%{_bindir}/*
%{_libdir}/*.so.*

%files devel
%{_includedir}/*
%{_libdir}/*.so

%changelog
* Tue Apr 19 2016 Evgeniy Polyakov <zbr@ioremap.net> - 0.1.1
- Added all connect/join logic
- Implemented simple distribute map
- Tests

* Tue Apr 19 2016 Evgeniy Polyakov <zbr@ioremap.net> - 0.1.0
- Initial commit

