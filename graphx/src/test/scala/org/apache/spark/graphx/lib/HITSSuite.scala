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

import org.apache.spark.SparkFunSuite
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd._


class HITSSuite extends SparkFunSuite with LocalSparkContext {

  def compareScores(a: VertexRDD[(Double, Double)], b: VertexRDD[(Double, Double)]): Double = {
    a.leftJoin(b) {
      case (id, (hub1, auth1), ha2Opt) => ha2Opt.getOrElse((0.0, 0.0)) match {
        case (hub2, auth2) => (hub1 - hub2) * (hub1 - hub2) + (auth1 - auth2) * (auth1 - auth2)
      }
    }.map { case (id, error) => error }.sum()
  }

  test("Star HITS") {
    withSpark { sc =>
      val nVertices = 10
      val starGraph = GraphGenerators.starGraph(sc, nVertices).cache()
      val errorTol = 1.0e-5

      val staticHA1 = starGraph.staticHITS(1).vertices.cache()
      val staticHA2 = starGraph.staticHITS(2).vertices.cache()

      // Static HITS should only take 1 iteration to converge
      assert(compareScores(staticHA1, staticHA2) < errorTol)

      val referenceHA = VertexRDD(sc.parallelize(
        (0 until nVertices).map {
          x => if (x == 0) {
            (x.toLong, (0.0, 1.0))
          } else {
            (x.toLong, (1.0 / sqrt(nVertices - 1), 0.0))
          }
        })).cache()

      assert(compareScores(staticHA1, referenceHA) < errorTol)
    }
  } // end of test Star HITS

  test("Reverse Star HITS") {
    withSpark { sc =>
      val nVertices = 10
      val starGraph = GraphGenerators.starGraph(sc, nVertices).reverse.cache()
      val errorTol = 1.0e-5

      val staticHA1 = starGraph.staticHITS(1).vertices.cache()
      val staticHA2 = starGraph.staticHITS(2).vertices.cache()

      // Static HITS should only take 1 iteration to converge
      assert(compareScores(staticHA1, staticHA2) < errorTol)

      val referenceHA = VertexRDD(sc.parallelize(
        (0 until nVertices).map {
          x => if (x == 0) {
            (x.toLong, (1.0, 0.0))
          } else {
            (x.toLong, (0.0, 1.0 / sqrt(nVertices - 1)))
          }
        })).cache()

      assert(compareScores(staticHA1, referenceHA) < errorTol)
    }
  } // end of test Reverse Star HITS

  test("Chain HITS") {
    withSpark { sc =>
      val nVertices = 10
      val chain1 = (0 until (nVertices - 1)).map(x => (x, x + 1))
      val rawEdges = sc.parallelize(chain1, 1).map { case (s, d) => (s.toLong, d.toLong) }
      val chain = Graph.fromEdgeTuples(rawEdges, 1.0).cache()
      val errorTol = 1.0e-5

      val staticHA1 = chain.staticHITS(1).vertices.cache()
      val staticHA2 = chain.staticHITS(2).vertices.cache()

      // Static HITS should only take 1 iteration to converge
      assert(compareScores(staticHA1, staticHA2) < errorTol)

      val referenceHA = VertexRDD(sc.parallelize(
        (0 until nVertices).map {
          x => if (x == nVertices - 1) {
            (x.toLong, (0.0, 1.0 / sqrt(nVertices - 1)))
          } else if (x == 0) {
            (x.toLong, (1.0 / sqrt(nVertices - 1), 0.0))
          } else {
            (x.toLong, (1.0 / sqrt(nVertices - 1), 1.0 / sqrt(nVertices - 1)))
          }
        })).cache()

      assert(compareScores(staticHA1, referenceHA) < errorTol)
    }
  }

  test("HITS on a Toy Graph") {
    withSpark { sc =>
      // Create an RDD for the vertices
      val users: RDD[(VertexId, (String, String))] =
        sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
                       (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
                       (4L, ("peter", "student"))))
      // Create an RDD for edges
      val relationships: RDD[Edge[String]] =
        sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
                       Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
                       Edge(4L, 0L, "student"), Edge(5L, 0L, "colleague")))
      // Edges are:
      //   2 ---> 5 ---> 3
      //          | \
      //          V   \|
      //   4 ---> 0    7
      //
      // Define a default user in case there are relationship with missing user
      val defaultUser = ("John Doe", "Missing")
      // Build the initial Graph
      val graph = Graph(users, relationships, defaultUser)
      val numIter = 20
      val errorTol = 1.0e-5

      val staticHA = graph.staticHITS(numIter).vertices.cache()
      val referenceHA = VertexRDD(sc.parallelize(
        List((0L, (0.0, 0.6279630)), (2L, (0.0, 0.0)), (3L, (0.3250576, 0.4597008)),
          (4L, (0.3250576, 0.0)), (5L, (0.8880738, 0.0)), (7L, (0.0, 0.6279630))))).cache()

      assert(compareScores(staticHA, referenceHA) < errorTol)
    }
  } // end of toy HITS

}
