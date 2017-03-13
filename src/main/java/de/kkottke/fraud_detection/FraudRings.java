package de.kkottke.fraud_detection;

import org.apache.commons.lang3.RandomStringUtils;
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
import org.apache.flink.util.Collector;

public class FraudRings {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<String, String, String>> input = env.readCsvFile("/Users/kkt/workspaces/fraud-detection/src/main/resources/input.txt")
                .types(String.class, String.class, String.class);
        DataSet<Tuple3<String, String, String>> vertices = input.flatMap(new VertexExtractor()).distinct(2);
        DataSet<Tuple2<String, String>> edges = input.flatMap(new EdgeExtractor());

        // open a delta iteration
        DeltaIteration<Tuple3<String, String, String>, Tuple3<String, String, String>> iteration =
                vertices.iterateDelta(vertices, 100, 2);

        // apply the step logic: join with the edges, select the minimum neighbor, update if the component of the candidate is smaller
        DataSet<Tuple3<String, String, String>> changes = iteration.getWorkset().join(edges).where(2).equalTo(0).with(new MessageWithComponentIDJoin())
                .groupBy(0).aggregate(Aggregations.MIN, 1)
                .join(iteration.getSolutionSet()).where(0).equalTo(2)
                .with(new ComponentIdFilter());

        // close the delta iteration (delta and new workset are identical)
        DataSet<Tuple3<String, String, String>> iterationResult = iteration.closeWith(changes, changes);

        DataSet<Tuple6<String, String, String, String, String, String>> result =
                iterationResult.filter(new FilterFunction<Tuple3<String, String, String>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, String> value) throws Exception {
                        return value.f1.equals("User");
                    }
                })
                        .join(edges).where(2).equalTo(0)
                        .join(iterationResult).where("f1.f1").equalTo(2).with(new VertexPair());
//        result.print();

//        DataSet<Tuple4<String, String, String, String[]>> result = iterationResult.coGroup(edges).where(2).equalTo(0).with(new VertexWithEdgesGroup());
//        result.print();

        writeToNeo4j(result);
        env.execute();
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    public static final class VertexExtractor implements FlatMapFunction<Tuple3<String, String, String>, Tuple3<String, String, String>> {

        Tuple3<String, String, String> source = new Tuple3<>();
        Tuple3<String, String, String> target = new Tuple3<>();

        @Override
        public void flatMap(Tuple3<String, String, String> line, Collector<Tuple3<String, String, String>> out) {
            source.f0 = RandomStringUtils.randomAlphanumeric(10);
            source.f1 = "User";
            source.f2 = line.f0;
            out.collect(source);

            target.f0 = RandomStringUtils.randomAlphanumeric(10);
            target.f1 = line.f1;
            target.f2 = line.f2;
            out.collect(target);
        }
    }

    public static final class EdgeExtractor implements FlatMapFunction<Tuple3<String, String, String>, Tuple2<String, String>> {

        Tuple2<String, String> edge1 = new Tuple2<>();
        Tuple2<String, String> edge2 = new Tuple2<>();

        @Override
        public void flatMap(Tuple3<String, String, String> line, Collector<Tuple2<String, String>> out) {
            edge1.f0 = line.f0;
            edge1.f1 = line.f2;
            out.collect(edge1);

            edge2.f0 = line.f2;
            edge2.f1 = line.f0;
            out.collect(edge2);
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("f0->f1")
    @FunctionAnnotation.ForwardedFieldsSecond("f1->f0")
    public static final class MessageWithComponentIDJoin implements JoinFunction<Tuple3<String, String, String>, Tuple2<String, String>, Tuple2<String, String>> {

        @Override
        public Tuple2<String, String> join(Tuple3<String, String, String> vertex, Tuple2<String, String> edge) {
            return new Tuple2<>(edge.f1, vertex.f0);
        }
    }

    public static final class ComponentIdFilter implements FlatJoinFunction<Tuple2<String, String>, Tuple3<String, String, String>, Tuple3<String, String, String>> {

        @Override
        public void join(Tuple2<String, String> message, Tuple3<String, String, String> vertex, Collector<Tuple3<String, String, String>> out) {
            if (message.f1.compareTo(vertex.f0) < 0) {
                vertex.f0 = message.f1;
                out.collect(vertex);
            }
        }
    }

    public static final class VertexPair implements JoinFunction<Tuple2<Tuple3<String, String, String>, Tuple2<String, String>>, Tuple3<String, String, String>, Tuple6<String, String, String, String, String, String>> {
        Tuple6<String, String, String, String, String, String> result = new Tuple6<>();

        @Override
        public Tuple6<String, String, String, String, String, String> join(
                Tuple2<Tuple3<String, String, String>, Tuple2<String, String>> vertexWithEdge, Tuple3<String, String, String> vertex) {
            result.f0 = vertexWithEdge.f0.f0;
            result.f1 = vertexWithEdge.f0.f1;
            result.f2 = vertexWithEdge.f0.f2;
            result.f3 = vertex.f0;
            result.f4 = vertex.f1;
            result.f5 = vertex.f2;

            return result;
        }
    }

    private static void writeToNeo4j(DataSet<Tuple6<String, String, String, String, String, String>> result) {
        Neo4jOutputFormat.Builder outputBuilder = Neo4jOutputFormat
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
                .addParameterKey(0, "ringId1")
                .addParameterKey(1, "type1")
                .addParameterKey(2, "name1")
                .addParameterKey(3, "ringId2")
                .addParameterKey(4, "type2")
                .addParameterKey(5, "name2")
                .setTaskBatchSize(100);
        Neo4jOutputFormat<Tuple6<String, String, String, String, String, String>> userOutput = outputBuilder.finish();
        result.output(userOutput).setParallelism(1);

//        Neo4jOutputFormat<Tuple6<String, String, String, String, String, String>> mailOutput = outputBuilder
//                .setCypherQuery("UNWIND {inserts} AS i " +
//                        "MERGE (user:User {name: i.name1, ringId: i.ringId1}) " +
//                        "MERGE (prop:Address {name: i.name2, ringId: i.ringId2}) " +
//                        "MERGE (user)-[:has]->(prop)").finish();
//        result.filter(new TypeFilter("Address")).output(mailOutput);

//        Neo4jOutputFormat<Tuple6<String, String, String, String, String, String>> addressOutput = outputBuilder
//                .setCypherQuery("UNWIND {inserts} AS i " +
//                        "MERGE (user:User {name: i.name1, ringId: i.ringId1}) " +
//                        "MERGE (prop:Bank {name: i.name2, ringId: i.ringId2}) " +
//                        "MERGE (user)-[:has]->(prop)").finish();
//        result.filter(new TypeFilter("Bank-Account")).output(addressOutput);
//
//        Neo4jOutputFormat<Tuple6<String, String, String, String, String, String>> bankOutput = outputBuilder
//                .setCypherQuery("UNWIND {inserts} AS i " +
//                        "MERGE (user:User {name: i.name1, ringId: i.ringId1}) " +
//                        "MERGE (prop:Credit {name: i.name2, ringId: i.ringId2}) " +
//                        "MERGE (user)-[:has]->(prop)").finish();
//        result.filter(new TypeFilter("Credit-Card")).output(bankOutput);
    }

    private static final class TypeFilter implements FilterFunction<Tuple6<String, String, String, String, String, String>> {

        private final String type;

        private TypeFilter(String type) {
            this.type = type;
        }

        @Override
        public boolean filter(Tuple6<String, String, String, String, String, String> value) throws Exception {
            return type.equals(value.f4);
        }
    }
}
