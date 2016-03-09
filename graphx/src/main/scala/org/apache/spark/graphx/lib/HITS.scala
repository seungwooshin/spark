/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.graphx.lib

import scala.math.sqrt
import scala.reflect.ClassTag

import org.apache.spark.graphx._

/**
 * Hyperlink-Induced Topic Search (HITS) implementation.
 *
 * Given a graph, the HITS algorithm computes two scores for each vertex, namely
 * the hub score and the authority score. The hub score estimates the value of the
 * vertex's links to other vertices, whereas the authority score estimates the
 * value of the vertex's content. Hence, a good hub is a vertex that links to many
 * authorities, and a good authority is a vertex that is linked by many hubs.
 * 
 * The algorithm is straightforward and can be described as follows:
 *
 * Initialize every vertex's hub score and authority score to be 1.
 * For some fixed number of steps, repeat the following:
 * <ul>
 * <li> Update each vertex's authority score to be the sum of the hub scores
 *      of each node that links to it.
 * <li> Update each vertex's hub score to be the sum of the authority scores
 *      of each node that it links to.
 * <li> Normalize the hub scores by the square root of the sum of the squares
 *      of all hub scores.
 * <li> Normalize the authority scores by the square root of the sum of the squares
 *      of all authority scores.
 * </ul>
 */
object HITS {

  /**
   * Run the HITS algorithm for a fixed number of iterations returning
   * a graph with vertex attributes containing pairs of the hub score
   * and the authority score.
   *
   * @tparam VD the original vertex attribute (discarded in the computation)
   * @tparam ED the original edge attribute (preserved in the computation)
   *
   * @param graph the graph on which to run HITS
   * @param numIter the number of iterations of HITS to run
   *
   * @return a graph where each vertex has a pair of the hub score and
   *         the authority score as its attribute.
   */
  def run[VD: ClassTag, ED: ClassTag](
    graph: Graph[VD, ED], numIter: Int): Graph[(Double, Double), ED] =
  {
    // Initialize the hubs and authoroities graph with each vertex having
    // the hub score of 1 and the authority score of 1.
    var haGraph: Graph[(Double, Double), ED] = graph.mapVertices { case (_, _) => (1, 1) }

    val srcOnly = new TripletFields(true, false, false)
    val dstOnly = new TripletFields(false, true, false)

    var iteration = 0
    var intermediateGraph1: Graph[(Double, Double), ED] = null
    var intermediateGraph2: Graph[(Double, Double), ED] = null
    var intermediateGraph3: Graph[(Double, Double), ED] = null
    while (iteration < numIter) {
      haGraph.cache()

      // Update authority scores using hub scores of in-neighbors.
      intermediateGraph1 = haGraph
      val authorityUpdates = haGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr._1), _ + _, srcOnly)
      haGraph = haGraph.joinVertices(authorityUpdates) {
        case (id, (oldHub, oldAuthority), newAuthority) => (oldHub, newAuthority)
      }.cache()

      // Update hub scores using authority scores of out-neighbors.
      intermediateGraph2 = haGraph
      val hubUpdates = haGraph.aggregateMessages[Double](
        ctx => ctx.sendToSrc(ctx.dstAttr._2), _ + _, dstOnly)
      haGraph = haGraph.joinVertices(hubUpdates) {
        case (id, (oldHub, oldAuthority), newHub) => (newHub, oldAuthority)
      }.cache()

      // Compute the sum of squares of each score and then renormalize.
      intermediateGraph3 = haGraph
      val (_, (sumHubSquare, sumAuthSquare)) = haGraph.vertices
        .mapValues {
          ha => (ha._1 * ha._1, ha._2 * ha._2)
        }.reduce {
          case ((_, (hub1, authority1)), (_, (hub2, authority2))) =>
            (0, ((hub1 + hub2), (authority1 + authority2)))
        }
      haGraph = haGraph.mapVertices {
        case (_, (hub, authority)) =>
          (hub / sqrt(sumHubSquare), authority / sqrt(sumAuthSquare))
      }.cache()

      // Unpersist intermediate RDDs for faster garbage collection
      materialize(haGraph)
      unpersist(intermediateGraph1)
      unpersist(intermediateGraph2)
      unpersist(intermediateGraph3)
      
      iteration += 1
    }

    haGraph
  }

  /**
   * Forces materialization of a Graph
   */
  private def materialize(g: Graph[_, _]): Unit = {
    g.vertices.foreachPartition(x => {})
    g.edges.foreachPartition(x => {})
  }

  /**
   * Unpersists the vertices and the edges of a Graph
   */
  private def unpersist(g: Graph[_, _]): Unit = {
    g.vertices.unpersist(false)
    g.edges.unpersist(false)
  }

}
