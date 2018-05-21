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
import org.apache.tinkerpop.gremlin.process.computer.clustering.ClusterPopulationMapReduce;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.util.*;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marc de Lignie
 */
public class WeakComponentsVertexProgramTest extends AbstractVertexProgramTest {

    @Test
    @LoadGraphWith(MODERN)
    public void shouldExecuteWeakComponentsWithIterationsBreak() throws Exception {
        if (graphProvider.getGraphComputer(graph).features().supportsResultGraphPersistCombination(GraphComputer.ResultGraph.NEW, GraphComputer.Persist.VERTEX_PROPERTIES)) {
            final ComputerResult result = graph.
                compute(graphProvider.getGraphComputer(graph).getClass()).
                program(WeakComponentsVertexProgram.build().maxIterations(2).create(graph)).
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

    @Test
    @LoadGraphWith(MODERN)
    public void shouldExecuteWeakComponentsWithClusterCount() throws Exception {
        if (graphProvider.getGraphComputer(graph).features().supportsResultGraphPersistCombination(GraphComputer.ResultGraph.NEW, GraphComputer.Persist.VERTEX_PROPERTIES)) {
            final ComputerResult result = graph.
                    compute(graphProvider.getGraphComputer(graph).getClass()).
                    program(WeakComponentsVertexProgram.build().create(graph)).
                    mapReduce(ClusterCountMapReduce.build().create()).
                    submit().get();
            assertEquals(1, (int)result.memory().get("clusterCount"));
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldExecuteWeakComponentsWithFullyMeshedComponent() throws Exception {
        if (graphProvider.getGraphComputer(graph).features().supportsResultGraphPersistCombination(GraphComputer.ResultGraph.NEW, GraphComputer.Persist.VERTEX_PROPERTIES)) {
            addFullyMeshedComponent(graph, 5);
            final ComputerResult result = graph.
                compute(graphProvider.getGraphComputer(graph).getClass()).
                program(WeakComponentsVertexProgram.build().create(graph)).
                mapReduce(ClusterPopulationMapReduce.build().create()).
                submit().get();
            final Map<Integer, Long> expected = new HashMap<Integer, Long>() {{
                put(1, 6L);
                put(100, 5L);
            }};
            final Map<Integer, Long> actual = result.memory().get("clusterPopulation");
            assertEquals(expected.get(1), actual.get(1));
            assertEquals(expected.get(0), actual.get(0));
            assertEquals(3, result.memory().getIteration());
        }
    }

    private void addFullyMeshedComponent(Graph graph, int size) {
        List<Vertex> vertices = new ArrayList();
        for (int i = 0; i < size; i++) {
            vertices.add(graph.addVertex(T.id, 100 + i));
            for (int j = 0; j < i; j++) {
                vertices.get(i).addEdge("link", vertices.get(j));
            }
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldExecuteWeakComponentsWithLinearComponent() throws Exception {
        if (graphProvider.getGraphComputer(graph).features().supportsResultGraphPersistCombination(GraphComputer.ResultGraph.NEW, GraphComputer.Persist.VERTEX_PROPERTIES)) {
            addLinearComponent(graph, 10);
            final ComputerResult result = graph.
                    compute(graphProvider.getGraphComputer(graph).getClass()).
                    program(WeakComponentsVertexProgram.build().create(graph)).
                    mapReduce(ClusterPopulationMapReduce.build().create()).
                    submit().get();
            final Map<Integer, Long> expected = new HashMap<Integer, Long>() {{
                put(1, 6L);
                put(100, 10L);
            }};
            Map<Integer, Long> actual = result.memory().get("clusterPopulation");
            assertEquals(expected.get(1), actual.get(1));
            assertEquals(expected.get(0), actual.get(0));
            assertEquals(10, result.memory().getIteration());
        }
    }

    private void addLinearComponent(Graph graph, int size) {
        final List<Vertex> vertices = new ArrayList();
        for (int i = 0; i < size; i++) {
            vertices.add(graph.addVertex(T.id, 100 + i));
            if ( i > 0 ) vertices.get(i).addEdge("link", vertices.get(i-1));
        }
    }
}