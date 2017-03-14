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
import org.apache.flink.graph.gsa.*;
import org.apache.flink.types.NullValue;

public class FraudRingDetection_GSA {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple4<String, String, String, String>> input = env.readCsvFile("/Users/kkt/workspaces/fraud-detection/src/main/resources/input.txt")
                .types(String.class, String.class, String.class, String.class);
        DataSet<Tuple2<String, Tuple2<String, String>>> vertices = input.flatMap(new VertexExtractor()).distinct(0);
        DataSet<Tuple3<String, String, NullValue>> edges = input.flatMap(new EdgeExtractor(true));

        Graph<String, Tuple2<String, String>, NullValue> graph = Graph.fromTupleDataSet(vertices, edges, env);

        GSAConfiguration config = new GSAConfiguration();
        config.setDirection(EdgeDirection.ALL);
        Graph<String, Tuple2<String, String>, NullValue> resultGraph = graph.runGatherSumApplyIteration(
                new GenerateLabel(), new ChooseMinLabel(), new UpdateLabel(), 100, config);

        DataSet<Triplet<String, Tuple2<String, String>, NullValue>> result = resultGraph.getTriplets();
        result.print();
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    public static final class GenerateLabel extends GatherFunction<Tuple2<String, String>, NullValue, String> {

        @Override
        public String gather(Neighbor<Tuple2<String, String>, NullValue> neighbor) {
            return neighbor.getNeighborValue().f1;
        }
    }

    public static final class ChooseMinLabel extends SumFunction<Tuple2<String, String>, NullValue, String> {

        @Override
        public String sum(String label1, String label2) {
            return label1.compareTo(label2) < 0 ? label1 : label2;
        }
    }

    public static final class UpdateLabel extends ApplyFunction<String, Tuple2<String, String>, String> {

        @Override
        public void apply(String newValue, Tuple2<String, String> currentValue) {
            if (newValue.compareTo(currentValue.f1) < 0) {
                currentValue.f1 = newValue;
                setResult(currentValue);
            }
        }
    }
}
