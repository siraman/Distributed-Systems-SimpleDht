package edu.buffalo.cse.cse486586.simpledht;


public class Node {
    private String nodeId;
    private String nodePort;
    private String nodeSuccessor;
    private String nodeSuccessorPort;
    private String nodePredecessor;
    private String nodePredecessorPort;

    public Node() {
        this.nodeId = "";
        this.nodePort = "";
        this.nodeSuccessor = "";
        this.nodeSuccessorPort = "";
        this.nodePredecessor = "";
        this.nodePredecessorPort = "";
    }

//    public Node(String[] message) {
//        this.nodeId = message[0];
//        this.nodePort = message[1];
//        this.nodeSuccessor = message[2];
//        this.nodeSuccessorPort = message[3];
//        this.nodePredecessor = message[4];
//        this.nodePredecessorPort = message[5];
//    }

    public String getNodeSuccessorPort() {
        return nodeSuccessorPort;
    }

    public void setNodeSuccessorPort(String nodeSuccessorPort) {
        this.nodeSuccessorPort = nodeSuccessorPort;
    }

    public String getNodePredecessorPort() {
        return nodePredecessorPort;
    }

    public void setNodePredecessorPort(String nodePredecessorPort) {
        this.nodePredecessorPort = nodePredecessorPort;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getNodeSuccessor() {
        return nodeSuccessor;
    }

    public void setNodeSuccessor(String nodeSuccessor) {
        this.nodeSuccessor = nodeSuccessor;
    }

    public String getNodePredecessor() {
        return nodePredecessor;
    }

    public void setNodePredecessor(String nodePredecessor) {
        this.nodePredecessor = nodePredecessor;
    }

    public String getNodePort() {
        return nodePort;
    }

    public void setNodePort(String nodePort) {
        this.nodePort = nodePort;
    }

//    @Override
//    public String toString() {
//        return  nodeId

    @Override
    public String toString() {
        return "Node{" +
                "nodeId='" + nodeId + '\'' +
                ", nodePort='" + nodePort + '\'' +
                ", nodeSuccessor='" + nodeSuccessor + '\'' +
                ", nodeSuccessorPort='" + nodeSuccessorPort + '\'' +
                ", nodePredecessor='" + nodePredecessor + '\'' +
                ", nodePredecessorPort='" + nodePredecessorPort + '\'' +
                '}';
    }
//                + ":" + nodePort
//                + ":" + nodeSuccessor
//                + ":" + nodeSuccessorPort
//                + ":" + nodeSuccessor
//                + ":" + nodePredecessorPort
//                + ":" + nodePredecessor;
//    }
}
