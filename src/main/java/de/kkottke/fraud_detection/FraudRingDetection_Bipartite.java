package de.kkottke.fraud_detection;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.bipartite.BipartiteEdge;
import org.apache.flink.graph.bipartite.BipartiteGraph;
import org.apache.flink.graph.gsa.*;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

public class FraudRingDetection_Bipartite {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple4<String, String, String, String>> input = env.readCsvFile("/Users/kkt/workspaces/fraud-detection/src/main/resources/input.txt")
                .types(String.class, String.class, String.class, String.class);
        DataSet<Vertex<String, Tuple2<String, String>>> topVertices = input.flatMap(new VertexExtractor(true, false)).distinct(0);
        DataSet<Vertex<String, Tuple2<String, String>>> bottomVertices = input.flatMap(new VertexExtractor(false, true)).distinct(0);
        DataSet<BipartiteEdge<String, String, NullValue>> edges = input.flatMap(new EdgeExtractor());

        BipartiteGraph<String, String, Tuple2<String, String>, Tuple2<String, String>, NullValue> bipartiteGraph =
                BipartiteGraph.fromDataSet(topVertices, bottomVertices, edges, env);
        Graph<String, Tuple2<String, String>, Tuple2<NullValue, NullValue>> graph = bipartiteGraph.projectionTopSimple();

        GSAConfiguration config = new GSAConfiguration();
        config.setDirection(EdgeDirection.ALL);
        Graph<String, Tuple2<String, String>, Tuple2<NullValue, NullValue>> resultGraph = graph.runGatherSumApplyIteration(
                new GenerateLabel(), new ChooseMinLabel(), new UpdateLabel(), 100, config);

        DataSet<Triplet<String, Tuple2<String, String>, Tuple2<NullValue, NullValue>>> result = resultGraph.getTriplets();
        result.print();
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    public static final class VertexExtractor implements FlatMapFunction<Tuple4<String, String, String, String>, Vertex<String, Tuple2<String, String>>> {

        private final boolean extractSource;
        private final boolean extractTarget;

        private Vertex<String, Tuple2<String, String>> source = new Vertex<>("", new Tuple2<>());
        private Vertex<String, Tuple2<String, String>> target = new Vertex<>("", new Tuple2<>());

        VertexExtractor(boolean extractSource, boolean extractTarget) {
            this.extractSource = extractSource;
            this.extractTarget = extractTarget;
        }

        @Override
        public void flatMap(Tuple4<String, String, String, String> line, Collector<Vertex<String, Tuple2<String, String>>> out) {
            if (extractSource) {
                source.f0 = line.f0;
                source.f1.f0 = line.f1;
                source.f1.f1 = RandomStringUtils.randomAlphanumeric(10);
                out.collect(source);
            }

            if (extractTarget) {
                target.f0 = line.f2;
                target.f1.f0 = line.f3;
                target.f1.f1 = RandomStringUtils.randomAlphanumeric(10);
                out.collect(target);
            }
        }
    }

    public static final class EdgeExtractor implements FlatMapFunction<Tuple4<String, String, String, String>, BipartiteEdge<String, String, NullValue>> {

        private BipartiteEdge<String, String, NullValue> edge1 = new BipartiteEdge<>("", "", NullValue.getInstance());

        @Override
        public void flatMap(Tuple4<String, String, String, String> line, Collector<BipartiteEdge<String, String, NullValue>> out) {
            edge1.f0 = line.f0;
            edge1.f1 = line.f2;
            out.collect(edge1);
        }
    }

    public static final class GenerateLabel extends GatherFunction<Tuple2<String, String>, Tuple2<NullValue, NullValue>, String> {

        @Override
        public String gather(Neighbor<Tuple2<String, String>, Tuple2<NullValue, NullValue>>  neighbor) {
            return neighbor.getNeighborValue().f1;
        }
    }

    public static final class ChooseMinLabel extends SumFunction<Tuple2<String, String>, Tuple2<NullValue, NullValue>, String> {

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
