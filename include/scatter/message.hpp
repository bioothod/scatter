#pragma once

#include "scatter/endian.hpp"
#include "scatter/exception.hpp"

#include <map>
#include <memory>
#include <string>
#include <sstream>
#include <vector>

#include <string.h>

namespace ioremap { namespace scatter {

enum {
	SCATTER_CMD_SERVER	= 0,
	SCATTER_CMD_JOIN,
	SCATTER_CMD_REMOTE_IDS,
	SCATTER_CMD_CLIENT	= 1024,
	__SCATTER_CMD_MAX
};

// this message is a reply
#define SCATTER_FLAGS_REPLY		(1<<0)

// send acknowledge to given request
#define SCATTER_FLAGS_NEED_ACK		(1<<1)

struct header {
	uint64_t		id;
	uint64_t		db;
	int			cmd;
	int			status;
	uint64_t		flags;
	uint64_t		size;

	header();

	void convert();

	std::string cmd_string() const;
	std::string flags_string() const;
};

class message {
public:
	typedef std::shared_ptr<std::vector<char>> raw_buffer_t;

	// this member is always in CPU byte order
	// data in @buffer() can be either in CPU or LE order
	header			hdr;

	enum {
		header_size = sizeof(header)
	};

	message();
	message(size_t size);
	message(std::shared_ptr<std::vector<char>> raw_buffer);
	message(const message &other);

	bool decode_header();
	bool encode_header();

	uint64_t id() const;
	uint64_t flags() const;
	uint64_t db() const;

	void resize(size_t size);

	const char *buffer() const;
	char *buffer();

	const char *data() const;
	char *data();

	void append(const char *ptr, size_t size);

	raw_buffer_t raw_buffer() const;

	size_t total_size() const;
	size_t headroom() const;

	void advance(size_t size);

	std::string to_string() const;

private:
	// this buffer has to have enough size to host header and data
	raw_buffer_t m_buffer;

	bool m_cpu = true;
	size_t m_data_offset = 0;

	void convert_header(header *h);
};

}} // namespace ioremap::scatter
