package de.kkottke.fraud_detection.util;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

public class EdgeExtractor implements FlatMapFunction<Tuple4<String, String, String, String>, Tuple3<String, String, NullValue>> {

    private boolean directed;

    private Tuple3<String, String, NullValue> edge1 = new Tuple3<>("", "", NullValue.getInstance());
    private Tuple3<String, String, NullValue> edge2 = new Tuple3<>("", "", NullValue.getInstance());

    public EdgeExtractor(boolean directed) {
        this.directed = directed;
    }

    @Override
    public void flatMap(Tuple4<String, String, String, String> line, Collector<Tuple3<String, String, NullValue>> out) {
        edge1.f0 = line.f0;
        edge1.f1 = line.f2;
        out.collect(edge1);

        if (!directed) {
            edge2.f0 = line.f2;
            edge2.f1 = line.f0;
            out.collect(edge2);
        }
    }
}
