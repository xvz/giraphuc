#include <stdlib.h>
#include <fstream>
#include <iostream>
#include <string>
#include <cerrno>

static void usage(char **argv) {
  std::cout << "usage: " << argv[0] << " input-file output-file" << std::endl;
}

/**
 * Strips weights from a weighted adjacency list graph.
 *
 * The input graph must be in the adjacency list format, i.e.,
 * src dst1 weight1 dst2 weight2 ...
 *
 * The output graph will be in adjacency list format as well:
 * src dst1 dst2 ...
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
  
  std::cout.sync_with_stdio(false);    // don't flush on \n

  // longs, just to be safe
  long vertex_id, edge_dst;
  long long edge_weight;

  while (ifs >> vertex_id) {
    ofs << vertex_id;
    while ( (ifs.peek() != '\n') && (ifs >> edge_dst && ifs >> edge_weight) ) {
      ofs << " " << edge_dst;
    }
    ofs << "\n";
  }

  ifs.close();
  ofs.flush();
  ofs.close();
  return 0;
}
