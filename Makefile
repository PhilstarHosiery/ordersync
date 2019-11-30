all:
	clang++ -o ordersync -std=c++17 -O2 -I/usr/local/include -L/usr/local/lib -lpqxx -lpq src/dbfReader.cpp src/ordersync.cpp

install:
	cp ordersync /storage/philstar/bin/phsystem/

clean:
	rm ordersync
