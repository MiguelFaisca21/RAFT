package Threads;

import Structures.Labels;
import Structures.Message;

import javafx.util.Pair;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import static Server.Server.openConnectionToReplica;

public class ReadAndReplyThread implements Runnable{

    private HashMap<Integer, Socket> incomingSockets;
    private HashMap<Integer, Socket> outgoingSockets;
    private final HashMap<Integer, Pair<String, Integer>> replicasInfo;
    private Socket socket;
    private Set<String> dataSet;
    private final int replicaId;
    private final int otherReplicaId;

    public ReadAndReplyThread(int replicaId, int otherReplicaId, HashMap<Integer, Socket> incomingSockets,
                              HashMap<Integer, Socket> outgoingSockets, HashMap<Integer, Pair<String, Integer>> replicasInfo, Set<String> dataSet) {
        this.replicaId = replicaId;
        this.otherReplicaId = otherReplicaId;
        this.incomingSockets = incomingSockets;
        this.outgoingSockets = outgoingSockets;
        this.socket = incomingSockets.get(otherReplicaId);
        this.replicasInfo = replicasInfo;
        this.dataSet = dataSet;
    }

    @Override
    public void run() {
        processRequests();
    }
    private void processRequests() {
        while(true){
            try {
                System.out.println("<-- Reply THREAD: Reading for requests at "+socket+ " -->");
                Message requestMessage = (Message) new ObjectInputStream(socket.getInputStream()).readObject();
                System.out.println("<-- Reply THREAD: Message received. -->");
                if(requestMessage.getLabel() == Labels.ADD){
                    System.out.println("|-- Received ADD Request from Replica " + requestMessage.getSenderId() +" --|");
                    synchronized (dataSet){
                        if (!dataSet.contains(requestMessage.getData().get(0))){
                            dataSet.add(requestMessage.getData().get(0));
                            System.out.println("|-- Added " + requestMessage.getData() + " to the data set --|");
                            Message replyMessage = new Message(replicaId, requestMessage.getSenderId(), Labels.ADD_OK, null);
                            new ObjectOutputStream(socket.getOutputStream()).writeObject(replyMessage);
                        } else {
                            Message replyMessage = new Message(replicaId, requestMessage.getSenderId(), Labels.ADD_ALREADY_EXISTS, null);
                            new ObjectOutputStream(socket.getOutputStream()).writeObject(replyMessage);
                        }
                        System.out.println("|-- Sent ADD ACK Reply to Replica " + requestMessage.getSenderId() +" --|");
                    }
                } else if(requestMessage.getLabel() == Labels.GET){
                    synchronized (dataSet){
                        System.out.println("|-- Received GET Request from Replica " + requestMessage.getSenderId() +" --|");
                        Message replyMessage = new Message(replicaId, requestMessage.getSenderId(), Labels.GET_OK, dataSet.stream().collect(ArrayList::new, ArrayList::add, ArrayList::addAll));
                        new ObjectOutputStream(socket.getOutputStream()).writeObject(replyMessage);
                        System.out.println("|-- Sent GET Reply to Replica " + requestMessage.getSenderId() +" --|");
                    }
                }
            } catch (IOException e) {
                System.out.println("Error in using outgoing socket. Replica " + otherReplicaId + " is possibly down.");
                retryConnection();
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }
    private void retryConnection() {
        try {
            Socket newSocket = openConnectionToReplica(replicaId, otherReplicaId, replicasInfo, outgoingSockets, dataSet);
            System.out.println("OUTGOING SOCKET UPDATED: Reconnect to Replica " + otherReplicaId + " with socket " + newSocket);
            this.socket = incomingSockets.get(otherReplicaId);
            Thread.sleep(1000);
        } catch (IOException e) {
            System.out.println("!-- Retrying connection to Replica " + otherReplicaId + " --!");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public Socket getSocket() {
        return this.socket;
    }
}
