#include <stdlib.h>
#include <fstream>
#include <iostream>
#include <string>
#include <cerrno>
#include <random>

static void usage(char **argv) {
  std::cout << "usage: " << argv[0] << " input-file output-file" << std::endl;
}

/**
 * Reduces the numeric value of the weights for weighted, undirected graphs
 * used in MST. The output graph will NOT have unique weights.
 *
 * The input graph must be in the adjacency list format, i.e.,
 * src dst1 weight1 dst2 weight2 ...
 *
 * NOTE: Does not sort anything!
 */
int main(int argc, char **argv) {
  if ( argc < 3 ) {
    usage(argv);
    return -1;
  }

  std::ifstream ifs(argv[1], std::ifstream::in);
  std::ofstream ofs(argv[2], std::ofstream::out);

  if (!ifs || !ofs) {
    usage(argv);
    return -1;
  }
  
  std::random_device rd; // obtain a random number from hardware
  std::mt19937 eng(rd()); // seed the generator
  std::uniform_int_distribution<> distr(1, 5000); // define the range

  std::cout.sync_with_stdio(false);    // don't flush on \n

  // longs, just to be safe
  long vertex_id, edge_dst;
  long long edge_weight;
  while (ifs >> vertex_id) {
    ofs << vertex_id;
    while ( (ifs.peek() != '\n') && (ifs >> edge_dst && ifs >> edge_weight) ) {
      ofs << " " << edge_dst << " " << distr(eng);
    }
    ofs << "\n";
  }

  ifs.close();
  ofs.flush();
  ofs.close();
  return 0;
}
