package de.kkottke.fraud_detection.util;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

public class VertexExtractor implements FlatMapFunction<Tuple4<String, String, String, String>, Tuple2<String, Tuple2<String, String>>> {

    private Tuple2<String, Tuple2<String, String>> source = new Tuple2<>("", new Tuple2<>());
    private Tuple2<String, Tuple2<String, String>> target = new Tuple2<>("", new Tuple2<>());

    @Override
    public void flatMap(Tuple4<String, String, String, String> line, Collector<Tuple2<String, Tuple2<String, String>>> out) {
        source.f0 = line.f0;
        source.f1.f0 = line.f1;
        source.f1.f1 = RandomStringUtils.randomAlphanumeric(10);
        out.collect(source);

        target.f0 = line.f2;
        target.f1.f0 = line.f3;
        target.f1.f1 = RandomStringUtils.randomAlphanumeric(10);
        out.collect(target);
    }
}
