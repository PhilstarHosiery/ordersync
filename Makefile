all:
	clang++ -o ordersync -std=c++1y -O3 -I/usr/local/include -L/usr/local/lib -lboost_system -lpqxx -lpq src/dbfReader.cpp src/ordersync.cpp

install:
	cp ordersync /storage/philstar/bin/phsystem/

clean:
	rm ordersync
