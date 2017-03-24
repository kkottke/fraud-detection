package de.kkottke.fraud_detection.util;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

public class EdgeExtractor implements FlatMapFunction<Tuple2<String, String>, Tuple3<String, String, NullValue>> {

    private boolean directed;

    private Tuple3<String, String, NullValue> edge = new Tuple3<>("", "", NullValue.getInstance());

    public EdgeExtractor(boolean directed) {
        this.directed = directed;
    }

    @Override
    public void flatMap(Tuple2<String, String> line, Collector<Tuple3<String, String, NullValue>> out) {
        edge.f0 = line.f0;
        edge.f1 = line.f1;
        out.collect(edge);

        if (!directed) {
            edge.f0 = line.f1;
            edge.f1 = line.f0;
            out.collect(edge);
        }
    }
}
