add_library(scatter exception.cpp)
target_link_libraries(scatter
	${FOLLY_LIBRARIES}
	${WANGLE_LIBRARIES}
	${MSGPACK_LIBRARIES}
)

#add_executable(scatter_node node.cpp)
#target_link_libraries(scatter_node scatter)
