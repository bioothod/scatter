#include "scatter/address.hpp"

namespace ioremap { namespace scatter {

address::address()
{
	memset(data, 0, sizeof(data));
	size = 0;
	family = 0;
}

address::address(const boost::asio::ip::address &addr, int port) : address()
{
	if (addr.is_v4()) {
		family = AF_INET;
		size = sizeof(struct sockaddr_in);

		auto bytes = addr.to_v4().to_bytes();

		struct sockaddr_in *in = (struct sockaddr_in *)data;
		in->sin_port = htons(port);
		memcpy(&in->sin_addr, bytes.data(), sizeof(in->sin_addr));
	} else {
		family = AF_INET6;
		size = sizeof(struct sockaddr_in6);

		auto bytes = addr.to_v6().to_bytes();

		struct sockaddr_in6 *in6 = (struct sockaddr_in6 *)data;
		in6->sin6_port = htons(port);
		memcpy(&in6->sin6_addr, bytes.data(), sizeof(in6->sin6_addr));
	}
}

address::address(const boost::asio::ip::tcp::endpoint &ep) : address(ep.address(), ep.port())
{
}

bool address::operator==(const address &other)
{
	return size == other.size && !memcmp(data, other.data, size);
}

int address::port() const
{
	if (size == sizeof(struct sockaddr_in) && family == AF_INET) {
		struct sockaddr_in *in = (struct sockaddr_in *)data;
		return ntohs(in->sin_port);
	} else if (size == sizeof(struct sockaddr_in6) && family == AF_INET6) {
		struct sockaddr_in6 *in = (struct sockaddr_in6 *)data;
		return ntohs(in->sin6_port);
	}

	return 0;
}

std::string address::host() const
{
	std::string tmp;
	tmp.resize(128);
	
	int err = getnameinfo((struct sockaddr *)data, size,
		(char *)tmp.data(), tmp.size(),
		NULL, 0, NI_NUMERICHOST | NI_NUMERICSERV);
	if (err) {
		return "invalid address, err: " + std::to_string(err);
	}

	tmp.resize(strlen(tmp.c_str()));
	return tmp;
}

std::string address::to_string() const
{
	return host() + ":" + std::to_string(port()) + ":" + std::to_string(family);
}

boost::asio::ip::tcp::endpoint address::endpoint() const
{
	if (family == AF_INET) {
		boost::asio::ip::address_v4::bytes_type bytes;
		struct sockaddr_in *in = (struct sockaddr_in *)data;
		memcpy(bytes.data(), &in->sin_addr, bytes.size());
		boost::asio::ip::address_v4 a(bytes);
		return boost::asio::ip::tcp::endpoint(a, port());
	} else {
		boost::asio::ip::address_v6::bytes_type bytes;
		struct sockaddr_in6 *in6 = (struct sockaddr_in6 *)data;
		memcpy(bytes.data(), &in6->sin6_addr, bytes.size());
		boost::asio::ip::address_v6 a(bytes);
		return boost::asio::ip::tcp::endpoint(a, port());
	}
}

void address::msgpack_unpack(msgpack::object o)
{
	int array_size = 2;
	if (o.type != msgpack::type::ARRAY || (int)o.via.array.size != array_size) {
		ioremap::scatter::throw_error(-EINVAL, "address unpack: type: %d, must be: %d, size: %d, must be: %d",
				o.type, msgpack::type::ARRAY, o.via.array.size, array_size);
	}

	msgpack::object *p = o.via.array.ptr;
	p[0].convert(&this->family);
	const char *data = p[1].via.raw.ptr;
	int size = p[1].via.raw.size;
	if ((size > (int)sizeof(this->data)) || ((family != AF_INET) && (family != AF_INET6))) {
		ioremap::scatter::throw_error(-EINVAL, "address unpack: family: %d, must be: %d or %d, raw.size: %d, must be less than: %zd",
				family, AF_INET, AF_INET6, size, sizeof(this->data));
	}
	
	memcpy(this->data, data, size);
	this->size = size;
}

}} // namespace ioremap::scatter
