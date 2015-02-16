all: snap-convert snap-revert mst-convert mst-weight-reducer mst-weight-remover

clean:
	rm -f snap-convert
	rm -f snap-revert
	rm -f mst-convert
	rm -f mst-weight-reducer
	rm -f mst-weight-remover

snap-convert: snap-convert.cpp
	g++ -Wall snap-convert.cpp -o snap-convert

snap-revert: snap-revert.cpp
	g++ -Wall snap-revert.cpp -o snap-revert

mst-convert: mst-convert.cpp
	g++ -Wall mst-convert.cpp -o mst-convert

mst-weight-reducer: mst-weight-reducer.cpp
	g++ -Wall --std=c++0x mst-weight-reducer.cpp -o mst-weight-reducer

mst-weight-remover: mst-weight-remover.cpp
	g++ -Wall --std=c++0x mst-weight-remover.cpp -o mst-weight-remover