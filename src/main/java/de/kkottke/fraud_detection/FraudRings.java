package de.kkottke.fraud_detection;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

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

        DataSet<Tuple4<String, String, String, String[]>> result = iterationResult.coGroup(edges).where(2).equalTo(0).with(new VertexWithEdgesGroup());

        result.print();
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

    public static final class VertexWithEdgesGroup implements CoGroupFunction<Tuple3<String, String, String>, Tuple2<String, String>, Tuple4<String, String, String, String[]>> {
        List<String> neighbors = new ArrayList<>();
        Tuple4<String, String, String, String[]> result = new Tuple4<>();

        @Override
        public void coGroup(Iterable<Tuple3<String, String, String>> vertices, Iterable<Tuple2<String, String>> edges,
                            Collector<Tuple4<String, String, String, String[]>> out) throws Exception {
            Tuple3<String, String, String> vertex = vertices.iterator().next();
            result.f0 = vertex.f0;
            result.f1 = vertex.f1;
            result.f2 = vertex.f2;

            neighbors.clear();
            for (Tuple2<String, String> edge : edges) {
                neighbors.add(edge.f1);
            }
            result.f3 = neighbors.toArray(new String[neighbors.size()]);
            out.collect(result);
        }
    }
}
