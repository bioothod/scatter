#pragma once

#include "scatter/exception.hpp"

#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <msgpack.hpp>

#include <iomanip>
#include <glog/logging.h>

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

		if (VLOG_IS_ON(3)) {
			std::ostringstream ss;
			ss << std::hex << std::setfill('0');
			for (int i = 0; i < size; ++i) {
				ss << std::setw(2) << static_cast<unsigned>(data[i]);
			}
			VLOG(3) << "pack size: " << size << ", data: " << ss.str();
		}
	}

	void msgpack_unpack(msgpack::object o);
};

}} // namespace ioremap::scatter
