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
 */
public class WeakComponentsVertexProgram<M extends Comparable & Serializable> implements VertexProgram<M> {


    public static final String COMPONENT = "gremlin.weakComponentsVertexProgram.weakComponent";
    private static final String CHANGED = "gremlin.weakComponentsVertexProgram.changed";
    private static final String HAS_SENT = "gremlin.weakComponentsVertexProgram.edgeCount";
    private static final String MAX_ITERATIONS = "gremlin.weakComponentsVertexProgram.maxIterations";
    private static final String PROPERTY = "gremlin.weakComponentsVertexProgram.property";

    private MessageScope.Local<M> inScope = MessageScope.Local.of(__::inE);
    private MessageScope.Local<M> outScope = MessageScope.Local.of(__::outE);
    private int maxIterations;
    private String property = COMPONENT;
    private Set<VertexComputeKey> vertexComputeKeys;
    private Set<MemoryComputeKey> memoryComputeKeys;

    private WeakComponentsVertexProgram() {
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        this.maxIterations = configuration.getInt(MAX_ITERATIONS, 20);
        this.property = configuration.getString(PROPERTY, COMPONENT);
        this.vertexComputeKeys = new HashSet<>(Arrays.asList(
                VertexComputeKey.of(this.property, false),
                VertexComputeKey.of(HAS_SENT, true)));
        this.memoryComputeKeys = new HashSet<>(Arrays.asList(
                MemoryComputeKey.of(CHANGED, Operator.sum, false, true)));
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
    public void setup(final Memory memory) {
        memory.set(CHANGED, 0L);
    }

    /*
    After an iteration:
        CHANGED holds the number of vertices that CHANGED their PROPERTY in this iteration
        HAS_SENT indicates whether a vertex has sent a message
    */
    @Override
    public void execute(final Vertex vertex, Messenger<M> messenger, final Memory memory) {
        if (memory.isInitialIteration()) {
            messenger.sendMessage(this.inScope, (M)(vertex.id()));
            messenger.sendMessage(this.outScope, (M)(vertex.id()));
            vertex.property(VertexProperty.Cardinality.single, this.property, vertex.id());
            vertex.property(VertexProperty.Cardinality.single, HAS_SENT, true);
            memory.add(CHANGED, 1L);
        } else {
            final List<M> receivedMessages = new ArrayList<>();
            messenger.receiveMessages().forEachRemaining(receivedMessages::add);
            final M receivedComponent = receivedMessages.stream().reduce(
                    null, (a, b) -> (M)ObjectUtils.min(a, b));
            System.out.println(String.format("receivedComponent: %d", receivedComponent));
            if (ObjectUtils.compare(receivedComponent, vertex.value(this.property), true) < 0) {
                messenger.sendMessage(this.inScope, receivedComponent);
                messenger.sendMessage(this.outScope, receivedComponent);
                vertex.property(VertexProperty.Cardinality.single, this.property, receivedComponent);
                if (vertex.<Boolean>value(HAS_SENT) == false) {
                    vertex.property(VertexProperty.Cardinality.single, HAS_SENT, true);
                    memory.add(CHANGED, 1L);
                }
            } else {
                if (vertex.<Boolean>value(HAS_SENT) == true) {
                    vertex.property(VertexProperty.Cardinality.single, HAS_SENT, false);
                    memory.add(CHANGED, -1L);
                }
            }
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        boolean terminate = memory.<Long>get(CHANGED) < 1 || memory.getIteration() >= this.maxIterations;
        return terminate;
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