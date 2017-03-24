package de.kkottke.fraud_detection;

import de.kkottke.fraud_detection.util.EdgeExtractor;
import de.kkottke.fraud_detection.util.VertexExtractor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageCombiner;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.types.NullValue;

public class FraudRingDetection_BSP {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<String, String>> input = env.readCsvFile("/Users/kkt/workspaces/fraud-detection/src/main/resources/input.txt")
                .types(String.class, String.class);
        DataSet<Tuple2<String, String>> vertices = input.flatMap(new VertexExtractor()).distinct(0);
        DataSet<Tuple3<String, String, NullValue>> edges = input.flatMap(new EdgeExtractor(false));

        Graph<String, String, NullValue> graph = Graph.fromTupleDataSet(vertices, edges, env);

        Graph<String, String, NullValue> resultGraph = graph.runVertexCentricIteration(new LabellingFunction(), new LabelCombiner(), 100);

        DataSet<Triplet<String, String, NullValue>> result = resultGraph.getTriplets().filter(new FilterFunction<Triplet<String, String, NullValue>>() {
            @Override
            public boolean filter(Triplet<String, String, NullValue> value) throws Exception {
                return value.getSrcVertex().getId().endsWith("user");
            }
        });
        result.print();

//        result.output(new GephiOutputFormat()).setParallelism(1);
//        env.execute();
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    public static final class LabellingFunction extends ComputeFunction<String, String, NullValue, String> {

        @Override
        public void compute(Vertex<String, String> vertex, MessageIterator<String> messages) throws Exception {
            String label = vertex.getValue();

            if (getSuperstepNumber() == 1) {
                sendMessageToAllNeighbors(label);
            } else {
                for (String message : messages) {
                    if (message.compareTo(label) < 0) {
                        label = message;
                    }
                }

                if (!vertex.getValue().equals(label)) {
                    setNewVertexValue(label);
                    sendMessageToAllNeighbors(label);
                }
            }
        }

    }

    public static final class LabelCombiner extends MessageCombiner<String, String> {

        @Override
        public void combineMessages(MessageIterator<String> messages) throws Exception {
            String minMessage = messages.next();
            while (messages.hasNext()) {
                String message = messages.next();
                if (message.compareTo(minMessage) < 0) {
                    minMessage = message;
                }
            }

            if (minMessage != null) {
                sendCombinedMessage(minMessage);
            }
        }
    }
}
