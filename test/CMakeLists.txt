add_executable(scatter_test_main main.cpp)
target_link_libraries(scatter_test_main
	${Boost_LIBRARIES}
	${GLOG_LIBRARIES}
	${GTEST_LIBRARIES}
	scatter
)

add_executable(scatter_test_dmap dmap.cpp)
target_link_libraries(scatter_test_dmap
	${Boost_LIBRARIES}
	${GLOG_LIBRARIES}
	${GTEST_LIBRARIES}
	scatter
)
