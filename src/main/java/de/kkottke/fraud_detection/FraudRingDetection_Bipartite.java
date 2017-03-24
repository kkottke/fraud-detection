package de.kkottke.fraud_detection;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
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

        DataSet<Tuple2<String, String>> input = env.readCsvFile("/Users/kkt/workspaces/fraud-detection/src/main/resources/input.txt")
                .types(String.class, String.class);
        DataSet<Vertex<String, String>> topVertices = input.flatMap(new VertexExtractor(true, false)).distinct(0);
        DataSet<Vertex<String, String>> bottomVertices = input.flatMap(new VertexExtractor(false, true)).distinct(0);
        DataSet<BipartiteEdge<String, String, NullValue>> edges = input.flatMap(new EdgeExtractor());

        BipartiteGraph<String, String, String, String, NullValue> bipartiteGraph =
                BipartiteGraph.fromDataSet(topVertices, bottomVertices, edges, env);
        Graph<String, String, Tuple2<NullValue, NullValue>> graph = bipartiteGraph.projectionTopSimple();

        Graph<String, String, Tuple2<NullValue, NullValue>> resultGraph = graph.runGatherSumApplyIteration(
                new GenerateLabel(), new ChooseMinLabel(), new UpdateLabel(), 100);

        DataSet<Triplet<String, String, Tuple2<NullValue, NullValue>>> result = resultGraph.getTriplets();
        result.print();
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    public static final class VertexExtractor implements FlatMapFunction<Tuple2<String, String>, Vertex<String, String>> {

        private final boolean extractSource;
        private final boolean extractTarget;

        private Vertex<String, String> vertex = new Vertex<>();

        VertexExtractor(boolean extractSource, boolean extractTarget) {
            this.extractSource = extractSource;
            this.extractTarget = extractTarget;
        }

        @Override
        public void flatMap(Tuple2<String, String> line, Collector<Vertex<String, String>> out) {
            if (extractSource) {
                vertex.f0 = line.f0;
                vertex.f1 = RandomStringUtils.randomAlphanumeric(10);
                out.collect(vertex);
            }

            if (extractTarget) {
                vertex.f0 = line.f1;
                vertex.f1 = RandomStringUtils.randomAlphanumeric(10);
                out.collect(vertex);
            }
        }
    }

    public static final class EdgeExtractor implements FlatMapFunction<Tuple2<String, String>, BipartiteEdge<String, String, NullValue>> {

        private BipartiteEdge<String, String, NullValue> edge1 = new BipartiteEdge<>("", "", NullValue.getInstance());

        @Override
        public void flatMap(Tuple2<String, String> line, Collector<BipartiteEdge<String, String, NullValue>> out) {
            edge1.f0 = line.f0;
            edge1.f1 = line.f1;
            out.collect(edge1);
        }
    }

    public static final class GenerateLabel extends GatherFunction<String, Tuple2<NullValue, NullValue>, String> {

        @Override
        public String gather(Neighbor<String, Tuple2<NullValue, NullValue>>  neighbor) {
            return neighbor.getNeighborValue();
        }
    }

    public static final class ChooseMinLabel extends SumFunction<String, Tuple2<NullValue, NullValue>, String> {

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
}
