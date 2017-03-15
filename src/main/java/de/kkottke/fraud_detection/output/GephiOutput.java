package de.kkottke.fraud_detection.output;

import org.gephi.graph.api.*;
import org.gephi.io.exporter.api.ExportController;
import org.gephi.project.api.ProjectController;
import org.openide.util.Lookup;

import java.io.File;
import java.io.IOException;

public class GephiOutput {

    public static void main(String[] args) throws IOException {
        ProjectController projectController = Lookup.getDefault().lookup(ProjectController.class);
        projectController.newProject();

        GraphModel graphModel = Lookup.getDefault().lookup(GraphController.class).getGraphModel();

        Node node1 = graphModel.factory().newNode("Node1");
        node1.setLabel("Label1");
        Node node2 = graphModel.factory().newNode("Node2");
        node2.setLabel("Label2");
        Edge edge = graphModel.factory().newEdge(node1, node2);

        DirectedGraph directedGraph = graphModel.getDirectedGraph();
        directedGraph.addNode(node1);
        directedGraph.addNode(node2);
        directedGraph.addEdge(edge);

        ExportController exportController = Lookup.getDefault().lookup(ExportController.class);
        exportController.exportFile(new File("test-graph.gexf"));
    }
}
