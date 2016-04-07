add_executable(scatter_test main.cpp)
target_link_libraries(scatter_test
	${Boost_LIBRARIES}
	${GLOG_LIBRARIES}
	${GTEST_LIBRARIES}
	scatter
)
