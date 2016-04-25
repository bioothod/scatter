Summary:	scatter - distributed messaging framework
Name:		scatter
Version:	0.2.0
Release:	1%{?dist}

License:	LGPL 3.0
Group: 		Development/Libraries
URL:		https://github.com/bioothod/scatter
Source0:	%{name}-%{version}.tar.bz2
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires:	boost-devel, boost-system, boost-thread, boost-program-options
BuildRequires:  cmake, msgpack-devel, glog-devel
BuildRequires:	ribosome-devel

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
* Mon Apr 25 2016 Evgeniy Polyakov <zbr@ioremap.net> - 0.2.0
- server: connection must be closed in drop() method, it will stop and clear its expiration thread.
- server: fixed broadcast groups announce when new server has connected to server, which currently handles group, which has to move
- server: fixed resolver use, since it is sync and return resolver::iterator now
- broadcast: use connection::shared_completion_t for broadcast completions, do not use vector in completion, since it can be updated in parallel, which may crash
- node: resolver is sync now and returns resolve::iterator
- connection: when connection is going down, its expiration module must be closed first,
- 	since it can have links (hold shared pointers) to conneciton which will prevent connection from being freed
- 	and will eventually timeout and crash, since connection and expiration thread will be freed from expiration thread itself.
- connection: cleanup message sending, move shared completion into public visible space,
- 	use common constructor for empty/self connection (added empty process/error callbacks)
- resolver: get rid of boost::asio::use_future, it can not be compiled with g++ 5.3.1, but it became synchronous
- server: close acceptor and all accepted connections before exiting, remove connection from accepted set when it is being closed
- test: added very basic connection test
- pool: wait until all pending tasks are completed
- node: close all connections before exiting, do not request connections if current connection already exists
- message: explicitly initialize all header fields
- route: return error insert/remove has failed, fail insertion if there exists connection with the same ids
- address: fixed family assignment, added verbose logging
- connection: added transaction expiration and test
- node: randomly initialize node's ID
- Added ribosome dependency
- test: fixed sign warnings
- package: do not depend on gtest, it is downloaded and installed by cmake instructions
- cmake: download and install gtest-1.7.0
- debian: fixed glog dependency
- Create README.md
- Updated license to LGPL-3.0

* Tue Apr 19 2016 Evgeniy Polyakov <zbr@ioremap.net> - 0.1.1
- Added all connect/join logic
- Implemented simple distribute map
- Tests

* Tue Apr 19 2016 Evgeniy Polyakov <zbr@ioremap.net> - 0.1.0
- Initial commit

