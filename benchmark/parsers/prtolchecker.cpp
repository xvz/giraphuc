#include <fstream>
#include <iostream>
#include <iomanip>
#include <cmath>


static void usage(char **argv) {
  std::cout << "usage: " << argv[0] << " first-PageRank-file second-PageRank-file" << std::endl;
}

/**
 * Checks max delta error between two PageRank outputs.
 */
int main(int argc, char **argv) {
  if ( argc < 3 ) {
    usage(argv);
    return -1;
  }

  std::ifstream ifs1(argv[1], std::ifstream::in);
  std::ifstream ifs2(argv[2], std::ifstream::in);

  if (!ifs1 || !ifs2) {
    usage(argv);
    return -1;
  }

  long id1, id2;
  double err1, err2;
  double l1norm;

  while (ifs1 >> id1 >> err1 && ifs2 >> id2 >> err2) {
    l1norm += std::abs(err1 - err2);
  }

  ifs1.close();
  ifs2.close();

  // 16 to account for "e-xx" exponents
  std::cout << std::scientific << std::setprecision(16) << l1norm << std::endl;
  return 0;
}
