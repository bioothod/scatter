#include "scatter/map.hpp"

namespace ioremap { namespace scatter {

dmap::dmap(int io_pool_size, uint64_t map_db) : m_node(io_pool_size), m_map_db(map_db)
{
}

void dmap::connect(const std::string &addr)
{
	auto cn = m_node.connect(addr, std::bind(&dmap::process, this, std::placeholders::_1, std::placeholders::_2));
	m_node.bcast_join(m_map_db);
}

void dmap::insert(const std::string &key, const dkey &data, connection::process_fn_t complete)
{
	local_insert(key, data);

	dmap_package pkg;
	pkg.keys.push_back({key, data});
	send_command(SCATTER_CMD_DMAP_INSERT, pkg, complete);
}

void dmap::remove(const std::string &key, const dkey &data, connection::process_fn_t complete)
{
	local_remove(key, data);

	dmap_package pkg;
	pkg.keys.push_back({key, data});
	send_command(SCATTER_CMD_DMAP_REMOVE, pkg, complete);
}

std::vector<dkey> dmap::find(const std::string &key)
{
	return local_find(key);
}

void dmap::local_insert(const std::string &key, const dkey &data)
{
	std::lock_guard<std::mutex> guard(m_lock);
	m_map.emplace(std::make_pair(key, data));
}

void dmap::local_remove(const std::string &key, const dkey &data)
{
	std::lock_guard<std::mutex> guard(m_lock);
	auto p = m_map.equal_range(key);
	for (auto it = p.first; it != m_map.end() && it != p.second; ++it) {
		if (it->first != key)
			break;

		if (it->second != data)
			continue;

		m_map.erase(it);
		break;
	}
}

std::vector<dkey> dmap::local_find(const std::string &key)
{
	std::vector<dkey> ret;

	std::lock_guard<std::mutex> guard(m_lock);
	auto p = m_map.equal_range(key);
	for (auto it = p.first; it != m_map.end() && it != p.second; ++it) {
		if (it->first != key)
			break;

		ret.push_back(it->second);
	}

	return ret;
}

void dmap::process(connection::pointer cn, message &msg)
{
	dmap_package pkg;

	try {
		msgpack::unpacked up;
		msgpack::unpack(&up, msg.data(), msg.hdr.size);

		up.get().convert(&pkg);

	} catch (const std::exception &e) {
		LOG(ERROR) << "connection: " << cn->connection_string() <<
			", message: " << msg.to_string() <<
			", error: could not unpack map package: " << e.what();

		msg.hdr.status = -EINVAL;
		return;
	}

	switch (msg.hdr.cmd) {
	case SCATTER_CMD_DMAP_INSERT:
		for (auto &k: pkg.keys) {
			local_insert(k.key, k.data);
		}
		break;
	case SCATTER_CMD_DMAP_REMOVE:
		for (auto &k: pkg.keys) {
			local_remove(k.key, k.data);
		}
		break;
	}
}

void dmap::send_command(int cmd, const dmap_package &pkg, connection::process_fn_t complete)
{
	std::stringstream buffer;
	msgpack::pack(buffer, pkg);
	std::string rdata(buffer.str());

	message msg(rdata.size());
	msg.hdr.flags = SCATTER_FLAGS_NEED_ACK;
	msg.hdr.size = rdata.size();
	msg.hdr.cmd = cmd;
	msg.hdr.db = m_map_db;
	msg.hdr.id = m_node.id();
	msg.append(rdata.data(), rdata.size());

	m_node.send(msg, complete);
}

}} // namespace ioremap::scatter
