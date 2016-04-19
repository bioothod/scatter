#pragma once

#include "scatter/node.hpp"

#include <msgpack.hpp>

namespace ioremap { namespace scatter {

struct dkey {
	std::string		bucket, key;

	bool operator==(const dkey &other) {
		return other.bucket == bucket && other.key == key;
	}
	bool operator!=(const dkey &other) {
		return !(*this == other);
	}

	MSGPACK_DEFINE(bucket, key);
};

struct dkey_package {
	std::string		key;
	dkey			data;

	MSGPACK_DEFINE(key, data);
};

struct dmap_package {
	std::vector<dkey_package> keys;

	MSGPACK_DEFINE(keys);
};

class dmap {
public:
	enum {
		SCATTER_CMD_DMAP_INSERT = SCATTER_CMD_CLIENT + 1,
		SCATTER_CMD_DMAP_REMOVE,
	} command;

	dmap(int io_pool_size, uint64_t map_db);

	void connect(const std::string &addr);

	void insert(const std::string &key, const dkey &data, connection::process_fn_t complete);
	void remove(const std::string &key, const dkey &data, connection::process_fn_t complete);
	std::vector<dkey> find(const std::string &key);

private:
	std::mutex m_lock;
	std::multimap<std::string, dkey> m_map;
	node m_node;
	uint64_t m_map_db;

	void local_insert(const std::string &key, const dkey &data);
	void local_remove(const std::string &key, const dkey &data);
	std::vector<dkey> local_find(const std::string &key);

	void process(connection::pointer cn, message &msg);
	void send_command(int cmd, const dmap_package &pkg, connection::process_fn_t complete);
};

}} // namespace ioremap::scatter
