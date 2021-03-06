#include "scatter/node.hpp"

#include <boost/algorithm/string.hpp>
#include <boost/program_options.hpp>

#include <iostream>

using namespace ioremap;

class simple_map {
public:
	simple_map() {
	}

	void connect(const std::string &s, uint64_t db) {
		auto c = m_node.connect(s, std::bind(&simple_map::process, this, std::placeholders::_1, std::placeholders::_2));

		m_node.bcast_join(db);
	}

	void send(const std::string &s, uint64_t db, scatter::connection::process_fn_t complete) {
		scatter::message msg(s.size() + 1);
		msg.hdr.db = db;
		msg.hdr.cmd = scatter::SCATTER_CMD_CLIENT + 1;
		msg.hdr.flags = SCATTER_FLAGS_NEED_ACK;
		msg.hdr.id = 123456;
		msg.hdr.size = s.size() + 1;
		msg.append(s.c_str(), s.size() + 1);

		m_node.send(msg, complete);
	}

private:
	scatter::node m_node;

	void process(scatter::connection::pointer client, scatter::message &msg) {
		LOG(INFO) << "connection: " << client->connection_string() <<
			", received message: " << msg.to_string() <<
			", received data: " << (char *)msg.data() ;
	}
};

int main(int argc, char *argv[])
{
	namespace bpo = boost::program_options;

	bpo::options_description generic("Scatter generic example options");
	generic.add_options()
		("help", "this help message")
		;

	std::vector<std::string> remotes;
	uint64_t db;

	bpo::options_description snet("Scatter network options");
	snet.add_options()
		("remote", bpo::value<std::vector<std::string>>(&remotes)->composing()->required(), "remote node: addr:port:family")
		("db", bpo::value<uint64_t>(&db)->required(), "numberic database id to use")
		;

	bpo::options_description cmdline_options;
	cmdline_options.add(generic).add(snet);

	bpo::variables_map vm;

	try {
		bpo::store(bpo::command_line_parser(argc, argv).options(cmdline_options).run(), vm);

		if (vm.count("help")) {
			std::cout << cmdline_options << std::endl;
			return 0;
		}

		bpo::notify(vm);
	} catch (const std::exception &e) {
		std::cerr << "Invalid options: " << e.what() << "\n" << cmdline_options << std::endl;
		return -1;
	}


	srand(time(NULL));

	simple_map n;

	for (const auto &addr : remotes) {
		n.connect(addr, db);

		time_t t = time(NULL);
		std::string s = "this is a test at " + std::string(ctime(&t));

		n.send(s, db, [s] (scatter::connection::pointer self, scatter::message &msg) {
					if (msg.hdr.status) {
						LOG(ERROR) << "could not write data, error status: " << msg.hdr.status;
						return;
					}

					std::cout << "successfully wrote data: " << s << std::endl;
				});
	}

	while (true) {
		sleep(1);
	}
}
