package de.kkottke.fraud_detection;

import de.kkottke.fraud_detection.util.EdgeExtractor;
import de.kkottke.fraud_detection.util.VertexExtractor;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.GSAConnectedComponents;
import org.apache.flink.types.NullValue;

public class FraudRingDetection_CC {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<String, String>> input = env.readCsvFile("/Users/kkt/workspaces/fraud-detection/src/main/resources/input.txt")
                .types(String.class, String.class);
        DataSet<Tuple2<String, String>> vertices = input.flatMap(new VertexExtractor()).distinct(0);
        DataSet<Tuple3<String, String, NullValue>> edges = input.flatMap(new EdgeExtractor(true));

        Graph<String, String, NullValue> graph = Graph.fromTupleDataSet(vertices, edges, env);

        DataSet<Vertex<String, String>> connectedVertices = graph.run(new GSAConnectedComponents<>(100));
        Graph<String, String, NullValue> resultGraph = Graph.fromTupleDataSet(
                vertices.join(connectedVertices).where(0).equalTo(0).with(new VerticesWithComponentIDJoin()), edges, env);

        DataSet<Triplet<String, String, NullValue>> result = resultGraph.getTriplets();
        result.print();
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    public static final class VerticesWithComponentIDJoin implements JoinFunction<Tuple2<String, String>, Vertex<String, String>, Tuple2<String, String>> {

        private Tuple2<String, String> result = new Tuple2<>();

        @Override
        public Tuple2<String, String> join(Tuple2<String, String> vertex, Vertex<String, String> compId) {
            result.f0 = vertex.f0;
            result.f1 = compId.f1;

            return result;
        }
    }
}
