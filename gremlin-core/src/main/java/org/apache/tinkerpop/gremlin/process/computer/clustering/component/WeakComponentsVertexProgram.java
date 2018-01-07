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

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.ObjectUtils;
import org.apache.tinkerpop.gremlin.process.computer.*;
import org.apache.tinkerpop.gremlin.process.computer.clustering.ClusterCountMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;
import java.util.*;


/**
 * @author Marc de Lignie
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class WeakComponentsVertexProgram<M extends Comparable & Serializable> implements VertexProgram<M> {


    private static final String MAX_ITERATIONS = "gremlin.weakComponentsVertexProgram.maxIterations";
    private static final String PROPERTY = "gremlin.weakComponentsVertexProgram.property";
    private static final String VOTE_TO_HALT = "gremlin.peerPressureVertexProgram.voteToHalt";

    private MessageScope.Local<M> inScope = MessageScope.Local.of(__::inE);
    private MessageScope.Local<M> outScope = MessageScope.Local.of(__::outE);
    private int maxIterations;
    private String property;
    private Set<VertexComputeKey> vertexComputeKeys;
    private Set<MemoryComputeKey> memoryComputeKeys;

    private WeakComponentsVertexProgram() {
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        this.maxIterations = configuration.getInt(MAX_ITERATIONS, 20);
        if (this.maxIterations < 2) {
            throw new IllegalArgumentException("The value of iterations should be at least 2");
        }
        this.property = configuration.getString(PROPERTY, ClusterCountMapReduce.CLUSTER);
        this.vertexComputeKeys = new HashSet<>(Arrays.asList(
            VertexComputeKey.of(this.property, false)));
        this.memoryComputeKeys = new HashSet<>(Arrays.asList(
            MemoryComputeKey.of(VOTE_TO_HALT, Operator.and, false, true)));
    }

    @Override
    public void storeState(final Configuration configuration) {
        VertexProgram.super.storeState(configuration);
        configuration.setProperty(MAX_ITERATIONS, this.maxIterations);
        configuration.setProperty(PROPERTY, this.property);
    }

    @Override
    public GraphComputer.ResultGraph getPreferredResultGraph() {
        return GraphComputer.ResultGraph.NEW;
    }

    @Override
    public GraphComputer.Persist getPreferredPersist() {
        return GraphComputer.Persist.VERTEX_PROPERTIES;
    }

    @Override
    public Set<VertexComputeKey> getVertexComputeKeys() {
        return this.vertexComputeKeys;
    }

    @Override
    public Set<MemoryComputeKey> getMemoryComputeKeys() {
        return this.memoryComputeKeys;
    }

    @Override
    public Optional<MessageCombiner<M>> getMessageCombiner() {
        return Optional.of(new MessageCombiner<M>() {
            @Override
            public M combine ( final M messageA, final M messageB){
                return (M)ObjectUtils.min(messageA, messageB);
            }
        });
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        final Set<MessageScope> set = new HashSet<>();
        set.add(this.inScope);
        set.add(this.outScope);
        return set;
    }

    @Override
    public WeakComponentsVertexProgram clone() {
            return this;
    }

    @Override
    public void setup(final Memory memory) { memory.set(VOTE_TO_HALT, false); } // Do not terminate after first iteration

    @Override
    public void execute(final Vertex vertex, Messenger<M> messenger, final Memory memory) {
        if (memory.isInitialIteration()) {
            messenger.sendMessage(this.inScope, (M)(vertex.id()));
            messenger.sendMessage(this.outScope, (M)(vertex.id()));
            vertex.property(VertexProperty.Cardinality.single, this.property, vertex.id());
        } else {
            final List<M> receivedMessages = new ArrayList<>();
            messenger.receiveMessages().forEachRemaining(receivedMessages::add);
            final M receivedComponent = receivedMessages.stream().reduce(
                    null, (a, b) -> (M)ObjectUtils.min(a, b));
            if (ObjectUtils.compare(receivedComponent, vertex.value(this.property), true) < 0) {
                messenger.sendMessage(this.inScope, receivedComponent);
                messenger.sendMessage(this.outScope, receivedComponent);
                memory.add(VOTE_TO_HALT, false);
                vertex.property(VertexProperty.Cardinality.single, this.property, receivedComponent);
            }
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        final boolean voteToHalt = memory.<Boolean>get(VOTE_TO_HALT) || memory.getIteration() >= this.maxIterations;
        if (voteToHalt) {
            return true;
        } else {
            memory.set(VOTE_TO_HALT, true);
            return false;
        }
    }

    @Override
    public String toString() {
        return StringFactory.vertexProgramString(this, "iterations=" + this.maxIterations);
    }

    //////////////////////////////

    public static Builder build() {
        return new Builder();
    }

    public final static class Builder extends AbstractVertexProgramBuilder<Builder> {

        private Builder() {
            super(WeakComponentsVertexProgram.class);
        }

        public Builder iterations(final int iterations) {
            this.configuration.setProperty(MAX_ITERATIONS, iterations);
            return this;
        }

        public Builder property(final String key) {
            this.configuration.setProperty(PROPERTY, key);
            return this;
        }

    }

    ////////////////////////////

    @Override
    public Features getFeatures() {
        return new Features() {
            @Override
            public boolean requiresLocalMessageScopes() {
                return true;
            }

            @Override
            public boolean requiresVertexPropertyAddition() {
                return true;
            }
        };
    }
}