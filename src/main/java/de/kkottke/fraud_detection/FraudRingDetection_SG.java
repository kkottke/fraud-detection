package de.kkottke.fraud_detection;

import de.kkottke.fraud_detection.util.EdgeExtractor;
import de.kkottke.fraud_detection.util.VertexExtractor;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;
import org.apache.flink.types.NullValue;

public class FraudRingDetection_SG {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple4<String, String, String, String>> input = env.readCsvFile("/Users/kkt/workspaces/fraud-detection/src/main/resources/input.txt")
                .types(String.class, String.class, String.class, String.class);
        DataSet<Tuple2<String, Tuple2<String, String>>> vertices = input.flatMap(new VertexExtractor()).distinct(0);
        DataSet<Tuple3<String, String, NullValue>> edges = input.flatMap(new EdgeExtractor(true));

        Graph<String, Tuple2<String, String>, NullValue> graph = Graph.fromTupleDataSet(vertices, edges, env);

        ScatterGatherConfiguration config = new ScatterGatherConfiguration();
        config.setDirection(EdgeDirection.ALL);
        Graph<String, Tuple2<String, String>, NullValue> resultGraph = graph.runScatterGatherIteration(new LabelMessenger(), new VertexLabelUpdater(), 100, config);

        DataSet<Triplet<String, Tuple2<String, String>, NullValue>> result = resultGraph.getTriplets();
        result.print();
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    public static final class LabelMessenger extends ScatterFunction<String, Tuple2<String, String>, String, NullValue> {

        @Override
        public void sendMessages(Vertex<String, Tuple2<String, String>> vertex) throws Exception {
            sendMessageToAllNeighbors(vertex.getValue().f1);
        }
    }

    public static final class VertexLabelUpdater extends GatherFunction<String, Tuple2<String, String>, String> {

        @Override
        public void updateVertex(Vertex<String, Tuple2<String, String>> vertex, MessageIterator<String> messages) throws Exception {
            String label = vertex.getValue().f1;

            for (String message : messages) {
                if (message.compareTo(label) < 0) {
                    label = message;
                }
            }

            if (!vertex.getValue().f1.equals(label)) {
                Tuple2<String, String> value = vertex.getValue();
                value.f1 = label;
                setNewVertexValue(value);
            }
        }
    }
}
