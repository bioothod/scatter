add_library(scatter SHARED
	exception.cpp
	connection.cpp
	message.cpp
	node.cpp
	server.cpp
)
target_link_libraries(scatter
	${FOLLY_LIBRARIES}
	${WANGLE_LIBRARIES}
	${MSGPACK_LIBRARIES}
)

#add_executable(scatter_node node.cpp)
#target_link_libraries(scatter_node scatter)
