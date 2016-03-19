project(scatter)
cmake_minimum_required(VERSION 2.6)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -W -Wall -Wextra -fstack-protector-all -g")

file(READ "${CMAKE_CURRENT_SOURCE_DIR}/debian/changelog" DEBCHANGELOG)

# version string: 0.3.12
# scatter major 0
# scatter minor 3.12
string(REGEX MATCH "([0-9]+\\.[0-9]+\\.[0-9]+)" DEBFULLVERSION "${DEBCHANGELOG}")
string(REGEX MATCH "([0-9]+)" EBUCKET_VERSION_MAJOR "${DEBFULLVERSION}")
string(REGEX MATCH "([0-9]+\\.[0-9]+$)" EBUCKET_VERSION_MINOR "${DEBFULLVERSION}")

include(cmake/locate_library.cmake)

LOCATE_LIBRARY(MSGPACK "msgpack.hpp" "msgpack")
LOCATE_LIBRARY(FOLLY "folly/futures/Future.h" "folly")
LOCATE_LIBRARY(GLOG "glog/logging.h" "glog")
LOCATE_LIBRARY(WANGLE "wangle/channel/AsyncSocketHandler.h" "wangle")

find_package(Boost COMPONENTS system thread program_options REQUIRED)
#find_package(Elliptics REQUIRED)

include_directories(
	"${CMAKE_CURRENT_SOURCE_DIR}/include"
	${FOLLY_INCLUDE_DIRS}
	${GLOG_INCLUDE_DIRS}
	${MSGPACK_INCLUDE_DIRS}
	${WANGLE_INCLUDE_DIRS}
)

link_directories(
	${FOLLY_LIBRARY_DIRS}
	${GLOG_LIBRARY_DIRS}
	${MSGPACK_LIBRARY_DIRS}
	${WANGLE_LIBRARY_DIRS}
)

#add_subdirectory(src)
add_subdirectory(example)

file(GLOB headers "${CMAKE_CURRENT_SOURCE_DIR}/include/scatter/*.hpp")
install(FILES ${headers} DESTINATION include/scatter)
