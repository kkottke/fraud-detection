package de.kkottke.fraud_detection.util;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class VertexExtractor implements FlatMapFunction<Tuple2<String, String>, Tuple2<String, String>> {

    private Tuple2<String, String> vertex = new Tuple2<>();

    @Override
    public void flatMap(Tuple2<String, String> line, Collector<Tuple2<String, String>> out) {
        fillVertex(line.f0);
        out.collect(vertex);

        fillVertex(line.f1);
        out.collect(vertex);
    }

    private void fillVertex(String id) {
        vertex.f0 = id;
        vertex.f1 = RandomStringUtils.randomAlphanumeric(10);
    }
}
