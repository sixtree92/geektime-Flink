package com.geekbang.flink.state.common;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import com.geekbang.flink.state.common.artificialstate.ComplexPayload;
import com.geekbang.flink.state.common.artificialstate.StatefulComplexPayloadSerializer;
import com.geekbang.flink.state.common.avro.ComplexPayloadAvro;
import com.geekbang.flink.state.common.avro.InnerPayLoadAvro;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.geekbang.flink.state.common.DataStreamAllroundTestJobFactory.*;
import static com.geekbang.flink.state.common.TestOperatorEnum.*;

/**
 * A general purpose test job for Flink's DataStream API operators and primitives.
 *
 * <p>The job is constructed of generic components from {@link DataStreamAllroundTestJobFactory}.
 * It currently covers the following aspects that are frequently present in Flink DataStream jobs:
 * <ul>
 *     <li>A generic Kryo input type.</li>
 *     <li>A state type for which we register a {@link KryoSerializer}.</li>
 *     <li>Operators with {@link ValueState}.</li>
 *     <li>Operators with union state.</li>
 *     <li>Operators with broadcast state.</li>
 * </ul>
 *
 * <p>The cli job configuration options are described in {@link DataStreamAllroundTestJobFactory}.
 *
 */
public class DataStreamAllroundTestProgram {

	public static void main(String[] args) throws Exception {
		final ParameterTool pt = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		setupEnvironment(env, pt);

		// add a keyed stateful map operator, which uses Kryo for state serialization
		DataStream<Event> eventStream = env
			.addSource(createEventSource(pt))
			.name(EVENT_SOURCE.getName())
			.uid(EVENT_SOURCE.getUid())
			.assignTimestampsAndWatermarks(createTimestampExtractor(pt))
			.keyBy(Event::getKey)
			.map(createArtificialKeyedStateMapper(
					// map function simply forwards the inputs
					(MapFunction<Event, Event>) in -> in,
					// state is verified and updated per event as a wrapped ComplexPayload state object
					(Event event, ComplexPayload lastState) -> {
							if (lastState != null && !lastState.getStrPayload().equals(KEYED_STATE_OPER_WITH_KRYO_AND_CUSTOM_SER.getName())
									&& lastState.getInnerPayLoad().getSequenceNumber() == (event.getSequenceNumber() - 1)) {
								System.out.println("State is set or restored incorrectly");
							}
							return new ComplexPayload(event, KEYED_STATE_OPER_WITH_KRYO_AND_CUSTOM_SER.getName());
						},
					Arrays.asList(
						new KryoSerializer<>(ComplexPayload.class, env.getConfig()), // KryoSerializer
						new StatefulComplexPayloadSerializer()), // custom stateful serializer
					Collections.singletonList(ComplexPayload.class) // KryoSerializer via type extraction
				)
			)
			.returns(Event.class)
			.name(KEYED_STATE_OPER_WITH_KRYO_AND_CUSTOM_SER.getName())
			.uid(KEYED_STATE_OPER_WITH_KRYO_AND_CUSTOM_SER.getUid());

		// add a keyed stateful map operator, which uses Avro for state serialization
		eventStream = eventStream
			.keyBy(Event::getKey)
			.map(createArtificialKeyedStateMapper(
					// map function simply forwards the inputs
					(MapFunction<Event, Event>) in -> in,
					// state is verified and updated per event as a wrapped ComplexPayloadAvro state object
					(Event event, ComplexPayloadAvro lastState) -> {
							if (lastState != null && !lastState.getStrPayload().equals(KEYED_STATE_OPER_WITH_AVRO_SER.getName())
									&& lastState.getInnerPayLoad().getSequenceNumber() == (event.getSequenceNumber() - 1)) {
								System.out.println("State is set or restored incorrectly");
							}

							ComplexPayloadAvro payload = new ComplexPayloadAvro();
							payload.setEventTime(event.getEventTime());
							payload.setInnerPayLoad(new InnerPayLoadAvro(event.getSequenceNumber()));
							payload.setStrPayload(KEYED_STATE_OPER_WITH_AVRO_SER.getName());
							payload.setStringList(Arrays.asList(String.valueOf(event.getKey()), event.getPayload()));

							return payload;
						},
					Collections.singletonList(
						new AvroSerializer<>(ComplexPayloadAvro.class)), // custom AvroSerializer
					Collections.singletonList(ComplexPayloadAvro.class) // AvroSerializer via type extraction
				)
			)
			.returns(Event.class)
			.name(KEYED_STATE_OPER_WITH_AVRO_SER.getName())
			.uid(KEYED_STATE_OPER_WITH_AVRO_SER.getUid());

		DataStream<Event> eventStream2 = eventStream
			.map(createArtificialOperatorStateMapper((MapFunction<Event, Event>) in -> in))
			.returns(Event.class)
			.name(OPERATOR_STATE_OPER.getName())
			.uid(OPERATOR_STATE_OPER.getUid());

		// apply a tumbling window that simply passes forward window elements;
		// this allows the job to cover timers state
		@SuppressWarnings("Convert2Lambda")
        DataStream<Event> eventStream3 = applyTumblingWindows(eventStream2.keyBy(Event::getKey), pt)
			.apply(new WindowFunction<Event, Event, Integer, TimeWindow>() {
				@Override
				public void apply(Integer integer, TimeWindow window, Iterable<Event> input, Collector<Event> out) {
					for (Event e : input) {
						out.collect(e);
					}
				}
			})
			.name(TIME_WINDOW_OPER.getName())
			.uid(TIME_WINDOW_OPER.getUid());

		eventStream3 = DataStreamAllroundTestJobFactory.verifyCustomStatefulTypeSerializer(eventStream3);

		if (isSimulateFailures(pt)) {
			eventStream3 = eventStream3
				.map(createFailureMapper(pt))
				.setParallelism(1)
				.name(FAILURE_MAPPER_NAME.getName())
				.uid(FAILURE_MAPPER_NAME.getUid());
		}

		eventStream3.keyBy(Event::getKey)
			.flatMap(createSemanticsCheckMapper(pt))
			.name(SEMANTICS_CHECK_MAPPER.getName())
			.uid(SEMANTICS_CHECK_MAPPER.getUid())
			.addSink(new PrintSinkFunction<>())
			.name(SEMANTICS_CHECK_PRINT_SINK.getName())
			.uid(SEMANTICS_CHECK_PRINT_SINK.getUid());

		// Check sliding windows aggregations. Output all elements assigned to a window and later on
		// check if each event was emitted slide_factor number of times
		DataStream<Tuple2<Integer, List<Event>>> eventStream4 = eventStream2.keyBy(Event::getKey)
			.window(createSlidingWindow(pt))
			.apply(new WindowFunction<Event, Tuple2<Integer, List<Event>>, Integer, TimeWindow>() {
				private static final long serialVersionUID = 3166250579972849440L;

				@Override
				public void apply(
                        Integer key, TimeWindow window, Iterable<Event> input,
                        Collector<Tuple2<Integer, List<Event>>> out) {

					out.collect(Tuple2.of(key, StreamSupport.stream(input.spliterator(), false).collect(Collectors.toList())));
				}
			})
			.name(SLIDING_WINDOW_AGG.getName())
			.uid(SLIDING_WINDOW_AGG.getUid());

		eventStream4.keyBy(events -> events.f0)
			.flatMap(createSlidingWindowCheckMapper(pt))
			.name(SLIDING_WINDOW_CHECK_MAPPER.getName())
			.uid(SLIDING_WINDOW_CHECK_MAPPER.getUid())
			.addSink(new PrintSinkFunction<>())
			.name(SLIDING_WINDOW_CHECK_PRINT_SINK.getName())
			.uid(SLIDING_WINDOW_CHECK_PRINT_SINK.getUid());

		env.execute("General purpose test job");
	}
}
