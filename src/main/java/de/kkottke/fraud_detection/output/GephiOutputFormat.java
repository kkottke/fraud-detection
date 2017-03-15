package de.kkottke.fraud_detection.output;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.gephi.graph.api.*;
import org.gephi.io.exporter.api.ExportController;
import org.gephi.project.api.ProjectController;
import org.openide.util.Lookup;

import java.awt.*;
import java.io.File;
import java.io.IOException;

public class GephiOutputFormat implements OutputFormat<Triplet<String, Tuple2<String, String>, NullValue>> {

    private int taskNumber;

    private GraphModel graphModel;
    private Column typeColumn;
    private Column compIdColumn;
    private DirectedGraph graph;

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        ProjectController projectController = Lookup.getDefault().lookup(ProjectController.class);
        projectController.newProject();

        this.taskNumber = taskNumber;
        this.graphModel = Lookup.getDefault().lookup(GraphController.class).getGraphModel();
        this.typeColumn = this.graphModel.getNodeTable().addColumn("type", String.class);
        this.compIdColumn = this.graphModel.getNodeTable().addColumn("compId", String.class);
        this.graph = graphModel.getDirectedGraph();
    }

    @Override
    public void writeRecord(Triplet<String, Tuple2<String, String>, NullValue> record) throws IOException {
        Node sourceNode = graph.getNode(record.getSrcVertex().getId());
        if (sourceNode == null) {
            sourceNode = createNode(record.getSrcVertex());
        }
        Node targetNode = graph.getNode(record.getTrgVertex().getId());
        if (targetNode == null) {
            targetNode = createNode(record.getTrgVertex());
        }
        Edge edge = graphModel.factory().newEdge(sourceNode, targetNode);

        graph.addNode(sourceNode);
        graph.addNode(targetNode);
        graph.addEdge(edge);
    }

    @Override
    public void close() throws IOException {
        ExportController exportController = Lookup.getDefault().lookup(ExportController.class);
        exportController.exportFile(new File("graph-" + taskNumber +".gexf"));
    }

    private Node createNode(Vertex<String, Tuple2<String, String>> vertex) {
        Node node = graphModel.factory().newNode(vertex.getId());
        node.setLabel(vertex.getId());
        node.setAttribute(typeColumn, vertex.getValue().f0);
        node.setAttribute(compIdColumn, vertex.getValue().f1);
        node.setColor(Color.blue);

        return node;
    }
}
