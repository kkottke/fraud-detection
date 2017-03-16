package de.kkottke.fraud_detection;

import de.kkottke.fraud_detection.util.EdgeExtractor;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.GSAConnectedComponents;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

public class FraudRingDetection_CC {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple4<String, String, String, String>> input = env.readCsvFile("/Users/kkt/workspaces/fraud-detection/src/main/resources/input.txt")
                .types(String.class, String.class, String.class, String.class);
        DataSet<Tuple3<String, String, String>> vertices = input.flatMap(new VertexExtractor()).distinct(0);
        DataSet<Tuple3<String, String, NullValue>> edges = input.flatMap(new EdgeExtractor(true));

        Graph<String, String, NullValue> graph = Graph.fromTupleDataSet(vertices.project(0, 2), edges, env);

        DataSet<Vertex<String, String>> connectedVertices = graph.run(new GSAConnectedComponents<>(100));
        Graph<String, Tuple2<String, String>, NullValue> resultGraph = Graph.fromTupleDataSet(
                vertices.join(connectedVertices).where(0).equalTo(0).with(new VerticesWithComponentIDJoin()), edges, env);

        DataSet<Triplet<String, Tuple2<String, String>, NullValue>> result = resultGraph.getTriplets();
        result.print();
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    public static final class VertexExtractor implements FlatMapFunction<Tuple4<String, String, String, String>, Tuple3<String, String, String>> {

        private Tuple3<String, String, String> source = new Tuple3<>();
        private Tuple3<String, String, String> target = new Tuple3<>();

        @Override
        public void flatMap(Tuple4<String, String, String, String> line, Collector<Tuple3<String, String, String>> out) {
            source.f0 = line.f0;
            source.f1 = line.f1;
            source.f2 = RandomStringUtils.randomAlphanumeric(10);
            out.collect(source);

            target.f0 = line.f2;
            target.f1 = line.f3;
            target.f2 = RandomStringUtils.randomAlphanumeric(10);
            out.collect(target);
        }
    }

    public static final class VerticesWithComponentIDJoin implements JoinFunction<Tuple3<String, String, String>, Vertex<String, String>, Tuple2<String, Tuple2<String, String>>> {

        private Tuple2<String, Tuple2<String, String>> result = new Tuple2<>("", new Tuple2<>());

        @Override
        public Tuple2<String, Tuple2<String, String>> join(Tuple3<String, String, String> vertex, Vertex<String, String> compId) {
            result.f0 = vertex.f0;
            result.f1.f0 = vertex.f1;
            result.f1.f1 = compId.f1;

            return result;
        }
    }
}
