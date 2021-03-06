#include "scatter/message.hpp"

#include <stdint.h>

namespace ioremap { namespace scatter {

header::header()
{
	trans = 0;
	id = 0;
	db = 0;
	cmd = 0;
	status = 0;
	flags = 0;
	size = 0;
}

void header::convert()
{
	trans = scatter_bswap64(trans);
	id = scatter_bswap64(id);
	db = scatter_bswap64(db);
	cmd = scatter_bswap32(cmd);
	status = scatter_bswap32(status);
	flags = scatter_bswap64(flags);
	size = scatter_bswap64(size);
}

std::string header::cmd_string() const
{
	if (cmd >= 0 && cmd < __SCATTER_CMD_MAX) {
		static std::map<int, std::string> command_strings = {
			{ SCATTER_CMD_SERVER, "[server]" },
			{ SCATTER_CMD_SERVER_JOIN, "[server_join]" },
			{ SCATTER_CMD_SERVER_LEAVE, "[server_leave]" },
			{ SCATTER_CMD_BCAST_JOIN, "[bcast_join]" },
			{ SCATTER_CMD_BCAST_LEAVE, "[bcast_leave]" },
			{ SCATTER_CMD_REMOTE_IDS, "[remote_ids]" },
			{ SCATTER_CMD_CONNECTIONS, "[connections]" },
			{ SCATTER_CMD_CLIENT, "[client]" },
		};

		return command_strings[cmd];
	}

	return "";
}

std::string header::flags_string() const
{
	static struct flag_info {
		uint64_t flag;
		const char *name;
	} infos[] = {
		{ SCATTER_FLAGS_REPLY, "reply" },
		{ SCATTER_FLAGS_NEED_ACK, "need_ack" },
	};

	std::ostringstream ss;
	ss << "[";
	std::string prefix = "";
	for (size_t i = 0; i < sizeof(infos) / sizeof(infos[0]); ++i) {
		if (flags & infos[i].flag) {
			ss << prefix << infos[i].name;
			prefix = "|";
		}
	}
	ss << "]";
	return ss.str();
}

message::message() : message(0)
{
}

message::message(size_t size)
{
	m_buffer = std::make_shared<std::vector<char>>(size + sizeof(header));
	advance(sizeof(header));
}

message::message(std::shared_ptr<std::vector<char>> raw_buffer) : m_buffer(raw_buffer)
{
}

message::message(const message &other) : m_buffer(other.m_buffer)
{
	hdr = other.hdr;
	m_cpu = other.m_cpu;
	m_data_offset = other.m_data_offset;
}

bool message::decode_header()
{
	hdr = *reinterpret_cast<header *>(buffer());
	convert_header(&hdr);
	return true;
}
bool message::encode_header()
{
	memcpy(buffer(), &hdr, sizeof(header));
	convert_header((header *)buffer());
	return true;
}
message::~message()
{
}

uint64_t message::trans() const
{
	return hdr.trans;
}

uint64_t message::flags() const
{
	return hdr.flags;
}

uint64_t message::db() const
{
	return hdr.db;
}

void message::resize(size_t size)
{
	m_buffer->resize(size + sizeof(header));
}

const char *message::buffer() const
{
	return m_buffer->data();
}
char *message::buffer()
{
	return const_cast<char *>(m_buffer->data());
}

const char *message::data() const
{
	return m_buffer->data() + m_data_offset;
}
char *message::data()
{
	return const_cast<char *>(m_buffer->data() + m_data_offset);
}

void message::append(const char *ptr, size_t size)
{
	if (size > total_size() - headroom()) {
		throw_error(-EINVAL, "append: data_offset: %zd, buffer_size: %zd, available: %zd, size: %zd, "
					"error: not enough size in data area",
				m_data_offset, m_buffer->size(), total_size() - headroom(), size);
	}

	memcpy(data(), ptr, size);
	m_data_offset += size;
}

message::raw_buffer_t message::raw_buffer() const
{
	return m_buffer;
}

size_t message::total_size() const
{
	return m_buffer->size();
}

size_t message::headroom() const
{
	return m_data_offset;
}

void message::advance(size_t size)
{
	if (m_data_offset + size > m_buffer->size()) {
		throw_error(-EINVAL, "advance: data_offset: %zd, buffer_size: %zd, size: %zd, error: too large size to advance",
				m_data_offset, m_buffer->size(), size);
	}

	m_data_offset += size;
}

std::string message::to_string() const
{
	std::ostringstream ss;
	ss <<	"[trans: " << hdr.trans <<
		", id: " << hdr.id <<
		", db: " << hdr.db <<
		", status: " << hdr.status <<
		", cmd: " << hdr.cmd << " " << hdr.cmd_string() <<
		", flags: 0x" << std::hex << hdr.flags << std::dec << " " << hdr.flags_string() <<
		", size: " << hdr.size <<
		"]";

	return ss.str();
}

void message::convert_header(header *h)
{
	if (!m_cpu)
		return;

	h->convert();
	m_cpu = false;
}

}} // namespace ioremap::scatter
