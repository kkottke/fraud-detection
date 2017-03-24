package de.kkottke.fraud_detection;

import de.kkottke.fraud_detection.util.EdgeExtractor;
import de.kkottke.fraud_detection.util.VertexExtractor;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.neo4j.Neo4jOutputFormat;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.gsa.*;
import org.apache.flink.types.NullValue;

public class FraudRingDetection_GSA {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<String, String>> input = env.readCsvFile("/Users/kkt/workspaces/fraud-detection/src/main/resources/input.txt")
                .types(String.class, String.class);
        DataSet<Tuple2<String, String>> vertices = input.flatMap(new VertexExtractor()).distinct(0);
        DataSet<Tuple3<String, String, NullValue>> edges = input.flatMap(new EdgeExtractor(true));

        Graph<String, String, NullValue> graph = Graph.fromTupleDataSet(vertices, edges, env);

        GSAConfiguration config = new GSAConfiguration();
        config.setDirection(EdgeDirection.ALL);
        Graph<String, String, NullValue> resultGraph = graph.runGatherSumApplyIteration(
                new GenerateLabel(), new ChooseMinLabel(), new UpdateLabel(), 100, config);

        DataSet<Triplet<String, String, NullValue>> result = resultGraph.getTriplets();
//        result.print();
        writeToNeo4j(result.project(0, 1, 2, 3));
        env.execute();
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    public static final class GenerateLabel extends GatherFunction<String, NullValue, String> {

        @Override
        public String gather(Neighbor<String, NullValue> neighbor) {
            return neighbor.getNeighborValue();
        }
    }

    public static final class ChooseMinLabel extends SumFunction<String, NullValue, String> {

        @Override
        public String sum(String label1, String label2) {
            return label1.compareTo(label2) < 0 ? label1 : label2;
        }
    }

    public static final class UpdateLabel extends ApplyFunction<String, String, String> {

        @Override
        public void apply(String newValue, String currentValue) {
            if (newValue.compareTo(currentValue) < 0) {
                setResult(newValue);
            }
        }
    }

    private static void writeToNeo4j(DataSet<Tuple4<String, String, String, String>> result) {
        Neo4jOutputFormat<Tuple4<String, String, String, String>> outputFormat = Neo4jOutputFormat
                .buildNeo4jOutputFormat()
                .setRestURI("http://192.168.99.100:7474/db/data/")
                .setUsername("neo4j")
                .setPassword("secret")
                .setConnectTimeout(1000)
                .setReadTimeout(1000)
                .setCypherQuery("UNWIND {inserts} AS i " +
                        "MERGE (user:User {name: i.name1, ringId: i.ringId1}) " +
                        "MERGE (prop:Property {name: i.name2, ringId: i.ringId2}) " +
                        "MERGE (user)-[:has]->(prop)")
                .addParameterKey("name1")
                .addParameterKey("name2")
                .addParameterKey("ringId1")
                .addParameterKey("ringId2")
                .setTaskBatchSize(100).finish();
        result.output(outputFormat).setParallelism(1);
    }
}
