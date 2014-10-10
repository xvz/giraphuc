/*
 * Copyright (c) 2009 Carnegie Mellon University.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://www.graphlab.ml.cmu.edu
 *
 */

#include <string>
#include <iostream>
#include <algorithm>
#include <vector>
#include <map>
#include <boost/unordered_map.hpp>
#include <time.h>

#include <graphlab.hpp>
#include <graphlab/graph/distributed_graph.hpp>

struct vdata {
  uint64_t labelid;
  vdata() :
      labelid(std::numeric_limits<uint64_t>::max()) {
  }

  void save(graphlab::oarchive& oarc) const {
    oarc << labelid;
  }
  void load(graphlab::iarchive& iarc) {
    iarc >> labelid;
  }
};

typedef graphlab::distributed_graph<vdata, graphlab::empty> graph_type;

//set label id at vertex id
void initialize_vertex(graph_type::vertex_type& v) {
  v.data().labelid = v.id();
}

//message where summation means minimum
struct min_message {
  uint64_t value;
  explicit min_message(uint64_t v) :
      value(v) {
  }
  min_message() :
      value(std::numeric_limits<uint64_t>::max()) {
  }
  min_message& operator+=(const min_message& other) {
    value = std::min<uint64_t>(value, other.value);
    return *this;
  }

  void save(graphlab::oarchive& oarc) const {
    oarc << value;
  }
  void load(graphlab::iarchive& iarc) {
    iarc >> value;
  }
};

class connected_component:
  public graphlab::ivertex_program<graph_type,
                                   graphlab::empty,
                                   min_message>,
  public graphlab::IS_POD_TYPE {
private:
  uint64_t min_labelid;
  bool do_scatter;
public:
  // have to use message passing (i.e., no gather)
  void init(icontext_type& context, const vertex_type& vertex,
            const message_type& msg) {
    min_labelid = msg.value;
  }

  // Do not gather
  edge_dir_type gather_edges(icontext_type& context,
                             const vertex_type& vertex) const {
    return graphlab::NO_EDGES;
  }

  // Do not gather
  //uint64_t gather(icontext_type& context, const vertex_type& vertex,
  //    edge_type& edge) const {
  //  return edge.source().data().labelid;
  //}

  // If labelid is updated, scatter/signal neighbours
  void apply(icontext_type& context, vertex_type& vertex,
             const graphlab::empty& empty) {
    do_scatter = false;

    if (min_labelid == std::numeric_limits<uint64_t>::max()) {
      do_scatter = true;   // no messages received yet, keep trying
    } else if (vertex.data().labelid > min_labelid) {
      vertex.data().labelid = min_labelid;
      do_scatter = true;
    }
  }

  // only scatter along out edges
  edge_dir_type scatter_edges(icontext_type& context,
                              const vertex_type& vertex) const {
    if (do_scatter) {
      return graphlab::OUT_EDGES;
    } else {
      return graphlab::NO_EDGES;
    }
  }

  // Signal neighbour only when their component id is larger
  // (otherwise we'll never terminate)
  void scatter(icontext_type& context, const vertex_type& vertex,
               edge_type& edge) const {
    if (vertex.data().labelid < edge.target().data().labelid ) {
      context.signal(edge.target(), min_message(vertex.data().labelid));
    }
  }
};

class graph_writer {
public:
  std::string save_vertex(graph_type::vertex_type v) {
    std::stringstream strm;
    strm << v.id() << "\t" << v.data().labelid << "\n";
    return strm.str();
  }
  std::string save_edge(graph_type::edge_type e) {
    return "";
  }
};

int main(int argc, char** argv) {
  graphlab::timer total_timer; total_timer.start();
  std::cout << "Connected Component\n\n";

  graphlab::mpi_tools::init(argc, argv);
  graphlab::distributed_control dc;
  global_logger().set_log_level(LOG_INFO);
  //parse options
  graphlab::command_line_options clopts("Connected Component.");
  std::string graph_dir;
  std::string saveprefix;
  std::string format = "adj";
  std::string exec_type = "synchronous";
  clopts.attach_option("graph", graph_dir,
                       "The graph file. This is not optional");
  clopts.add_positional("graph");
  clopts.attach_option("format", format,
                       "The graph file format");

  clopts.attach_option("engine", exec_type,
                       "The engine type synchronous or asynchronous");

  clopts.attach_option("saveprefix", saveprefix,
                       "If set, will save the pairs of a vertex id and "
                       "a component id to a sequence of files with prefix "
                       "saveprefix");

  if (!clopts.parse(argc, argv)) {
    dc.cout() << "Error in parsing command line arguments." << std::endl;
    return EXIT_FAILURE;
  }
  if (graph_dir == "") {
    std::cout << "--graph is not optional\n";
    return EXIT_FAILURE;
  }

  graph_type graph(dc, clopts);

  //load graph
  dc.cout() << "Loading graph in format: "<< format << std::endl;
  graph.load_format(graph_dir, format);
  graphlab::timer ti;
  graph.finalize();
  dc.cout() << "Finalization in " << ti.current_time() << std::endl;
  graph.transform_vertices(initialize_vertex);

  //running the engine
  //  time_t start, end;
  graphlab::omni_engine<connected_component> engine(dc, graph, exec_type, clopts);
  engine.signal_all();
  //  time(&start);
  engine.start();
  const double runtime = engine.elapsed_seconds();
  dc.cout() << "Finished Running engine in " << runtime
            << " seconds." << std::endl;

  //write results
  if (saveprefix.size() > 0) {
    graph.save(saveprefix, graph_writer(),
        false, //set to true if each output file is to be gzipped
        true, //whether vertices are saved
        false); //whether edges are saved
  }

  graphlab::mpi_tools::finalize();
  dc.cout() << "TOTAL TIME (sec): " << total_timer.current_time() << std::endl;
  return EXIT_SUCCESS;
}
