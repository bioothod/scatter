#include "scatter/scatter.hpp"

#include <boost/algorithm/string.hpp>
#include <boost/program_options.hpp>

using namespace ioremap;

class simple_map {
public:
	simple_map(const std::string &s) {
		m_node.reset(new scatter::node(create_addr(s)));
	}
	simple_map() {
		m_node.reset(new scatter::node(std::bind(&simple_map::process, this, std::placeholders::_1)));
	}

	void connect(const std::string &s, uint64_t db_id) {
		m_node->connect(create_addr(s), db_id);
	}

private:
	std::unique_ptr<scatter::node> m_node;

	std::unique_ptr<scatter::message> process(scatter::message &msg) {
		return std::unique_ptr<scatter::message>();
	}

	folly::SocketAddress create_addr(const std::string &s) const {
		std::vector<std::string> strs;
		boost::split(strs, s, boost::is_any_of(":"));
		if (strs.size() < 2)
			throw std::runtime_error("could not create socket address from '" + s + "'");

		return folly::SocketAddress(strs[0], atoi(strs[1].c_str()));
	}
};

int main(int argc, char *argv[])
{
	namespace bpo = boost::program_options;

	bpo::options_description generic("Scatter generic example options");
	generic.add_options()
		("help", "this help message")
		;

	std::string server_addr;
	std::vector<std::string> remotes;
	uint64_t db;

	bpo::options_description snet("Scatter network options");
	snet.add_options()
		("remote", bpo::value<std::vector<std::string>>(&remotes)->composing(), "remote node: addr:port:family")
		("listen", bpo::value<std::string>(&server_addr), "listen address: addr:port:family")
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


	if (server_addr.empty() && remotes.empty()) {
		std::cerr << "You must specify either address to listen or set of remote addresses to connect\n" <<
			cmdline_options << std::endl;
		return -1;
	}

	srand(time(NULL));

	std::unique_ptr<simple_map> n;
	if (!server_addr.empty()) {
		n.reset(new simple_map(server_addr));
	} else {
		n.reset(new simple_map());
	}

	for (const auto &addr : remotes) {
		n->connect(addr, db);
	}

	while (true) {
		sleep(1);
	}
}
