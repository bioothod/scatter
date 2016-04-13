#pragma once

#include "scatter/connection.hpp"

#include <boost/asio.hpp>
#include <boost/asio/ip/address_v4.hpp>
#include <boost/asio/ip/address_v6.hpp>

#include <msgpack.hpp>

namespace msgpack {

template <typename Stream>
static inline msgpack::packer<Stream> &operator <<(msgpack::packer<Stream> &o, const ioremap::scatter::connection::proto::endpoint &ep)
{
	size_t size = ep.size();
	char *data = (char *)ep.data();
	int family = ep.address().is_v4() ? AF_INET : AF_INET6;

	o.pack_array(4);
	o.pack(size);
	o.pack(family);
	o.pack(ep.port());
	o.pack_raw(size);
	o.pack_raw_body(data, size);

	return o;
}

static inline ioremap::scatter::connection::proto::endpoint &operator >>(msgpack::object o, ioremap::scatter::connection::proto::endpoint &ep)
{
	if (o.type != msgpack::type::ARRAY || o.via.array.size != 4) {
		std::ostringstream ss;
		ss << "unpack: type: " << o.type <<
			", must be: " << msgpack::type::ARRAY <<
			", size: " << o.via.array.size;
		throw std::runtime_error(ss.str());
	}

	object *p = o.via.array.ptr;
	size_t size;
	int family, port;
	p[0].convert(&size);
	p[1].convert(&family);
	p[2].convert(&port);
	const char *data = p[3].via.raw.ptr;

	if (family == AF_INET) {
		boost::asio::ip::address_v4::bytes_type bytes;
		memcpy((char *)bytes.data(), data, size);
		boost::asio::ip::address_v4 a(bytes);
		ep = ioremap::scatter::connection::proto::endpoint(a, port);
	} else {
		boost::asio::ip::address_v4::bytes_type bytes;
		memcpy((char *)bytes.data(), data, size);
		boost::asio::ip::address_v4 a(bytes);
		ep = ioremap::scatter::connection::proto::endpoint(a, port);
	}

	return ep;
}
} // namespace msgpack
