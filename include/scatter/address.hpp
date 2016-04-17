#pragma once

#include "scatter/exception.hpp"

#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <msgpack.hpp>

namespace ioremap { namespace scatter {

struct address {
	char		data[32];
	short		size;		// number of bytes in @data used to store low-level address representation
	short		family;		// AF_INET(2) or AF_INET6(10)

	address();
	address(const boost::asio::ip::address &addr, int port);
	address(const boost::asio::ip::tcp::endpoint &ep);

	bool operator==(const address &other);

	int port() const;
	std::string host() const;

	std::string to_string() const;

	boost::asio::ip::tcp::endpoint endpoint() const;

	// have to implement this templated method here, not in source file since we do not know what @Stream may look like
	template <typename Stream>
	void msgpack_pack(msgpack::packer<Stream> &o) const {
		o.pack_array(2);
		o.pack(family);
		o.pack_raw(size);
		o.pack_raw_body(data, size);
	}

	void msgpack_unpack(msgpack::object o);
};

}} // namespace ioremap::scatter
