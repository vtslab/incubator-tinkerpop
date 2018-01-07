/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.process.computer.clustering.component;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.computer.AbstractVertexProgramTest;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.clustering.ClusterCountMapReduce;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Marc de Lignie
 */
public class WeakComponentsVertexProgramTest extends AbstractVertexProgramTest {

    @Test
    @LoadGraphWith(MODERN)
    public void shouldExecuteWeakComponentsWithIterationsBreak() throws Exception {
        if (graphProvider.getGraphComputer(graph).features().supportsResultGraphPersistCombination(GraphComputer.ResultGraph.NEW, GraphComputer.Persist.VERTEX_PROPERTIES)) {
            final ComputerResult result = graph.
                compute(graphProvider.getGraphComputer(graph).getClass()).
                program(WeakComponentsVertexProgram.build().iterations(2).create(graph)).
                submit().get();
            final Set<Object> clusters = new HashSet<>();
            result.graph().traversal().V().forEachRemaining(v -> {
                assertEquals(3, v.keys().size()); // name, age/lang, component
                assertTrue(v.keys().contains("name"));
                assertTrue(v.keys().contains(ClusterCountMapReduce.CLUSTER));
                assertEquals(1, IteratorUtils.count(v.values("name")));
                assertEquals(1, IteratorUtils.count(v.values(ClusterCountMapReduce.CLUSTER)));
                final Object cluster = v.value(ClusterCountMapReduce.CLUSTER);
                clusters.add(cluster);
            });
            assertEquals(1, clusters.size());
            assertEquals(2, result.memory().getIteration());
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldExecuteWeakComponentsWithConvergenceBreak() throws Exception {
        if (graphProvider.getGraphComputer(graph).features().supportsResultGraphPersistCombination(GraphComputer.ResultGraph.NEW, GraphComputer.Persist.VERTEX_PROPERTIES)) {
            final ComputerResult result = graph.
                compute(graphProvider.getGraphComputer(graph).getClass()).
                program(WeakComponentsVertexProgram.build().create(graph)).
                submit().get();
            final Set<Object> clusters = new HashSet<>();
            result.graph().traversal().V().forEachRemaining(v -> {
                assertEquals(3, v.keys().size()); // name, age/lang, component
                assertTrue(v.keys().contains("name"));
                assertTrue(v.keys().contains(ClusterCountMapReduce.CLUSTER));
                assertEquals(1, IteratorUtils.count(v.values("name")));
                assertEquals(1, IteratorUtils.count(v.values(ClusterCountMapReduce.CLUSTER)));
                final Object cluster = v.value(ClusterCountMapReduce.CLUSTER);
                clusters.add(cluster);
            });
            assertEquals(1, clusters.size());
            assertEquals(3, result.memory().getIteration());
        }
    }

    //ToDo: add test for multiple clusters
    //ToDo: ClusterCountMapReduce and ClusterPopulationMapReduce tests in AbstractStorageCheck (already works manually)
    //Done: Vote_to_halt mechanisms
    //Done: tests in style of Pagerank
    //Done: gremlin-server test fails on org.apache.tinkerpop.gremlin.groovy.jsr223.RemoteGraphGroovyTranslatorProcessStandardTest
    //Done: better way for optouts in org/apache/tinkerpop/gremlin/process/remote/RemoteGraph.java:  instanceOf VertexProgram
}