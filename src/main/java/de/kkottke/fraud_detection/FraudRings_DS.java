package de.kkottke.fraud_detection;

import de.kkottke.fraud_detection.util.EdgeExtractor;
import de.kkottke.fraud_detection.util.VertexExtractor;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.io.neo4j.Neo4jOutputFormat;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

public class FraudRings_DS {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<String, String>> input = env.readCsvFile("/Users/kkt/workspaces/fraud-detection/src/main/resources/input.txt")
                .types(String.class, String.class);
        DataSet<Tuple2<String, String>> vertices = input.flatMap(new VertexExtractor()).distinct(0);
        DataSet<Tuple3<String, String, NullValue>> edges = input.flatMap(new EdgeExtractor(false));

        // open a delta iteration
        DeltaIteration<Tuple2<String, String>, Tuple2<String, String>> iteration =
                vertices.iterateDelta(vertices, 100, 0);

        // step logic:  join with the edges,
        //              select the minimum message,
        //              update if the component of the candidate is smaller
        DataSet<Tuple2<String, String>> changes =
                iteration.getWorkset()
                .join(edges).where(0).equalTo(0)
                        .with(new MessageWithComponentIDJoin()).name("Join_Edges")
                .groupBy(0).aggregate(Aggregations.MIN, 1).name("Min_Message")
                .join(iteration.getSolutionSet()).where(0).equalTo(0)
                        .with(new ComponentIdFilter()).name("Update");

        // close the delta iteration (delta and new workset are identical)
        DataSet<Tuple2<String, String>> iterationResult = iteration.closeWith(changes, changes);

        DataSet<Tuple6<String, String, String, String, String, String>> result =
                iterationResult.filter(new FilterFunction<Tuple2<String, String>>() {
                    @Override
                    public boolean filter(Tuple2<String, String> value) throws Exception {
                        return value.f0.endsWith("user");
                    }
                })
                        .join(edges).where(0).equalTo(0)
                        .join(iterationResult).where("f1.f1").equalTo(0).with(new VertexPair());
        result.print();

//        writeToNeo4j(result);
//        env.execute();
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    @FunctionAnnotation.ForwardedFieldsFirst("f0->f1")
    @FunctionAnnotation.ForwardedFieldsSecond("f1->f0")
    public static final class MessageWithComponentIDJoin implements JoinFunction<Tuple2<String, String>, Tuple3<String, String, NullValue>, Tuple2<String, String>> {

        @Override
        public Tuple2<String, String> join(Tuple2<String, String> vertex, Tuple3<String, String, NullValue> edge) {
            return new Tuple2<>(edge.f1, vertex.f1);
        }
    }

    public static final class ComponentIdFilter implements FlatJoinFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>> {

        @Override
        public void join(Tuple2<String, String> message, Tuple2<String, String> vertex, Collector<Tuple2<String, String>> out) {
            if (message.f1.compareTo(vertex.f1) < 0) {
                vertex.f1 = message.f1;
                out.collect(vertex);
            }
        }
    }

    public static final class VertexPair implements JoinFunction<Tuple2<Tuple2<String, String>, Tuple3<String, String, NullValue>>, Tuple2<String, String>, Tuple6<String, String, String, String, String, String>> {
        Tuple6<String, String, String, String, String, String> result = new Tuple6<>();

        @Override
        public Tuple6<String, String, String, String, String, String> join(
                Tuple2<Tuple2<String, String>, Tuple3<String, String, NullValue>> vertexWithEdge, Tuple2<String, String> vertex) {
            String[] idType = vertexWithEdge.f0.f0.split(":");
            result.f0 = idType[0];
            result.f1 = idType[1];
            result.f2 = vertexWithEdge.f0.f1;
            idType = vertex.f0.split(":");
            result.f3 = idType[0];
            result.f4 = idType[1];
            result.f5 = vertex.f1;

            return result;
        }
    }

    private static void writeToNeo4j(DataSet<Tuple6<String, String, String, String, String, String>> result) {
        Neo4jOutputFormat<Tuple6<String, String, String, String, String, String>> outputFormat = Neo4jOutputFormat
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
                .addParameterKey("type1")
                .addParameterKey("ringId1")
                .addParameterKey("name2")
                .addParameterKey("type2")
                .addParameterKey("ringId2")
                .setTaskBatchSize(100).finish();
        result.output(outputFormat).setParallelism(1);
    }

}
