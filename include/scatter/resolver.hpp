#pragma once

#include "scatter/exception.hpp"
#include "scatter/pool.hpp"

#include <boost/asio.hpp>
#include <boost/asio/use_future.hpp>

#include <iostream>

#include <glog/logging.h>

namespace ioremap { namespace scatter {

template <typename ProtoType = boost::asio::ip::tcp>
class resolver {
public:
	typedef typename ProtoType::resolver::iterator IterType;

	resolver(io_service_pool &io) : m_pool(io), m_resolver(io.get_service()) {}

	std::future<IterType> resolve(const std::string &addr_str) {
		std::string::size_type pos_end;
		pos_end = addr_str.rfind(":");
		if (pos_end == std::string::npos) {
			std::promise<IterType> p;
			std::future<IterType> f = p.get_future();

			p.set_exception(std::make_exception_ptr(
					create_error(-EINVAL, "invalid address string '%s', there is no ':' delimiter",
						addr_str.c_str())));
			return f;
		}

		int family = atoi(addr_str.substr(pos_end + 1).c_str());

		size_t pos_start;
		pos_end--;
		pos_start = addr_str.rfind(":", pos_end);
		if (pos_end == std::string::npos) {
			std::promise<IterType> p;
			std::future<IterType> f = p.get_future();

			p.set_exception(std::make_exception_ptr(
					create_error(-EINVAL, "invalid address string '%s', there is no ':' delimiter",
						addr_str.c_str())));
			return f;
		}

		std::string port = addr_str.substr(pos_start + 1, pos_end - pos_start);
		std::string addr = addr_str.substr(0, pos_start);

		return resolve(addr, port, family);
	}

	std::future<IterType> resolve(const std::string &addr_str, const std::string &port_str, int family = AF_INET) {
		VLOG(1) << "trying to resolve addr: " << addr_str << ", port: " << port_str << ", family: " << family << std::endl;

		auto proto = family == AF_INET6 ? ProtoType::v6() : ProtoType::v4();
		boost::system::error_code ec;
		auto addr = boost::asio::ip::address::from_string(addr_str, ec);
		if (!ec) {
			std::promise<IterType> p;
			std::future<IterType> f = p.get_future();

			auto endpoint = typename ProtoType::endpoint(addr, atoi(port_str.c_str()));
			p.set_value(IterType::create(endpoint, addr_str, port_str));
			return f;
		}

		typename ProtoType::resolver::query q{proto, addr_str, port_str};
		return m_resolver.async_resolve(q, boost::asio::use_future);
	}

private:
	io_service_pool &m_pool;
	typename ProtoType::resolver m_resolver;
};

}} // namespace ioremap::scatter
