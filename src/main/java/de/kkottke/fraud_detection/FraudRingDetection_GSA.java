package de.kkottke.fraud_detection;

import de.kkottke.fraud_detection.util.EdgeExtractor;
import de.kkottke.fraud_detection.util.VertexExtractor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.neo4j.Neo4jOutputFormat;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.*;
import org.apache.flink.graph.gsa.*;
import org.apache.flink.types.NullValue;

public class FraudRingDetection_GSA {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<String, String>> input = env.readCsvFile("/Users/kkt/Projekte/Flink/input/testdata.csv")
//        DataSet<Tuple2<String, String>> input = env.readCsvFile("/Users/kkt/workspaces/fraud-detection/src/main/resources/input.txt")
                .types(String.class, String.class);
        DataSet<Tuple2<String, String>> vertices = input.flatMap(new VertexExtractor()).distinct(0);
        DataSet<Tuple3<String, String, NullValue>> edges = input.flatMap(new EdgeExtractor(true));

        Graph<String, String, NullValue> graph = Graph.fromTupleDataSet(vertices, edges, env);

        GSAConfiguration config = new GSAConfiguration();
        config.setDirection(EdgeDirection.ALL);
        Graph<String, String, NullValue> resultGraph = graph.runGatherSumApplyIteration(
                new GenerateLabel(), new ChooseMinLabel(), new UpdateLabel(), 100, config);

        resultGraph.getVertices().map(new VertexResultMapper())
                .writeAsCsv("/Users/kkt/Projekte/Flink/output/nodes.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        resultGraph.getEdges().map(new EdgeResultMapper())
                .writeAsCsv("/Users/kkt/Projekte/Flink/output/edges.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
//        printTriplets(resultGraph);
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

    public static final class VertexResultMapper implements MapFunction<Vertex<String, String>, Tuple3<String, String, String>> {

        private Tuple3<String, String, String> tuple = new Tuple3<>();

        @Override
        public Tuple3<String, String, String> map(Vertex<String, String> vertex) throws Exception {
            String[] idType = vertex.getId().split(":");
            tuple.f0 = idType[0];
            tuple.f1 = idType[1];
            tuple.f2 = vertex.getValue();

            return tuple;
        }
    }

    public static final class EdgeResultMapper implements MapFunction<Edge<String, NullValue>, Tuple2<String, String>> {

        private Tuple2<String, String> tuple = new Tuple2<>();

        @Override
        public Tuple2<String, String> map(Edge<String, NullValue> edge) throws Exception {
            tuple.f0 = edge.getSource().split(":")[0];
            tuple.f1 = edge.getTarget().split(":")[0];

            return tuple;
        }
    }

    private static void printTriplets(Graph<String, String, NullValue> graph) throws Exception {
        DataSet<Triplet<String, String, NullValue>> result = graph.getTriplets();
        result.print();
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
