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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.geekbang.flink.state.common.artificialstate.builder;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.util.Preconditions;

/**
 * An {@link ArtificialStateBuilder} for user {@link ValueState}s.
 */
public class ArtificialValueStateBuilder<IN, STATE> extends ArtificialStateBuilder<IN> {

	private static final long serialVersionUID = -1205814329756790916L;

	private transient ValueState<STATE> valueState;
	private final ValueStateDescriptor<STATE> valueStateDescriptor;
	private final JoinFunction<IN, STATE, STATE> stateValueGenerator;

	public ArtificialValueStateBuilder(
		String stateName,
		JoinFunction<IN, STATE, STATE> stateValueGenerator,
		ValueStateDescriptor<STATE> valueStateDescriptor) {
		super(stateName);
		this.valueStateDescriptor = Preconditions.checkNotNull(valueStateDescriptor);
		this.stateValueGenerator = Preconditions.checkNotNull(stateValueGenerator);
	}

	@Override
	public void artificialStateForElement(IN event) throws Exception {
		valueState.update(stateValueGenerator.join(event, valueState.value()));
	}

	@Override
	public void initialize(FunctionInitializationContext initializationContext) {
		valueState = initializationContext.getKeyedStateStore().getState(valueStateDescriptor);
	}
}
