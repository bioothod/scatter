add_library(scatter SHARED
	broadcast.cpp
	connection.cpp
	exception.cpp
	message.cpp
	node.cpp
	pool.cpp
	route.cpp
	server.cpp
)
target_link_libraries(scatter
	${Boost_LIBRARIES}
	${FOLLY_LIBRARIES}
	${GLOG_LIBRARIES}
	${WANGLE_LIBRARIES}
	${MSGPACK_LIBRARIES}
)

add_executable(scatter_server server_main.cpp)
target_link_libraries(scatter_server scatter)
