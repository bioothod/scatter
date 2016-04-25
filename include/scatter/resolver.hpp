#pragma once

#include "scatter/exception.hpp"
#include "scatter/pool.hpp"

#include <boost/asio.hpp>

#include <iostream>

#include <glog/logging.h>

namespace ioremap { namespace scatter {

template <typename ProtoType = boost::asio::ip::tcp>
class resolver {
public:
	typedef typename ProtoType::resolver::iterator IterType;

	resolver(io_service_pool &io) : m_pool(io), m_resolver(io.get_service()) {}

	IterType resolve(const std::string &addr_str) {
		std::string::size_type pos_end;
		pos_end = addr_str.rfind(":");
		if (pos_end == std::string::npos) {
			throw_error(-EINVAL, "invalid address string '%s', there is no ':' delimiter", addr_str.c_str());
		}

		int family = atoi(addr_str.substr(pos_end + 1).c_str());

		size_t pos_start;
		pos_end--;
		pos_start = addr_str.rfind(":", pos_end);
		if (pos_end == std::string::npos) {
			throw_error(-EINVAL, "invalid address string '%s', there is no ':' delimiter", addr_str.c_str());
		}

		std::string port = addr_str.substr(pos_start + 1, pos_end - pos_start);
		std::string addr = addr_str.substr(0, pos_start);

		return resolve(addr, port, family);
	}

	IterType resolve(const std::string &addr_str, const std::string &port_str, int family = AF_INET) {
		VLOG(1) << "trying to resolve addr: " << addr_str << ", port: " << port_str << ", family: " << family << std::endl;

		auto proto = family == AF_INET6 ? ProtoType::v6() : ProtoType::v4();
		boost::system::error_code ec;
		auto addr = boost::asio::ip::address::from_string(addr_str, ec);
		if (!ec) {
			auto endpoint = typename ProtoType::endpoint(addr, atoi(port_str.c_str()));
			return IterType::create(endpoint, addr_str, port_str);
		}

		typename ProtoType::resolver::query q{proto, addr_str, port_str};
		std::promise<int> p;
		IterType res;
		std::future<int> f = p.get_future();

		m_resolver.async_resolve(q, [&] (const boost::system::error_code& ec, boost::asio::ip::tcp::resolver::iterator iterator) {
					if (ec) {
						p.set_exception(std::make_exception_ptr(
							create_error(ec.value(), "could not resolve '%s:%s:%d', error: %s",
								addr_str.c_str(), port_str.c_str(), family, ec.message().c_str())));
						return;
					}

					res = iterator;
					p.set_value(0);
				});

		f.get();
		return res;
	}

private:
	io_service_pool &m_pool;
	typename ProtoType::resolver m_resolver;
};

}} // namespace ioremap::scatter
