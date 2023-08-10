all: client-phase1 client-phase2 client-phase3 client-phase4 client-phase5

client-phase1: ./client-phase1.cpp
	g++ -std=c++17 ./client-phase1.cpp -o client-phase1
client-phase2: ./client-phase2.cpp
	g++ -std=c++17 ./client-phase2.cpp -o client-phase2
client-phase3: ./client-phase3.cpp
	g++ -std=c++17 ./client-phase3.cpp -o client-phase3 -lssl -lcrypto
client-phase4: ./client-phase4.cpp
	g++ -std=c++17 ./client-phase4.cpp -o client-phase4
client-phase5: ./client-phase5.cpp
	g++ -std=c++17 ./client-phase5.cpp -o client-phase5 -lssl -lcrypto