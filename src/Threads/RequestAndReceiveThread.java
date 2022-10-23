package Threads;

import Structures.Labels;
import Structures.Message;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

public class RequestAndReceiveThread implements Runnable {

    private HashMap<Integer, Socket> outgoingSockets;
    private Socket socket;
    private int replicaId;
    private int otherReplicaId;
    private Queue<Message> confirmQueue;
    private Queue<Message> responseQueue;
    private Queue<Message> bufferRequests = new LinkedList<>();

    private HashMap<Integer, Message> notFinishedBuffer;

    public RequestAndReceiveThread(int replicaId, int otherReplicaId, HashMap<Integer, Socket> outgoingSockets, Queue confirmQueue, Queue responseQueue, HashMap<Integer, Message> notFinishedBuffer) {
        this.replicaId = replicaId;
        this.otherReplicaId = otherReplicaId;
        this.outgoingSockets = outgoingSockets;
        this.socket = outgoingSockets.get(otherReplicaId);
        this.confirmQueue = confirmQueue;
        this.responseQueue = responseQueue;
        this.notFinishedBuffer = notFinishedBuffer;
    }

    @Override
    public void run() {
        while(true){
            synchronized (bufferRequests){
                while(bufferRequests.isEmpty()){
                    try {
                        System.out.println("<-- Request THREAD: Waiting for requests to use in "+ socket +" -->");
                        bufferRequests.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            System.out.println("<-- Request THREAD: Received Request. -->");
            this.socket = outgoingSockets.get(otherReplicaId);
            Message requestMessage = bufferRequests.poll();
            if(requestMessage.getLabel() == Labels.ADD){
                try {
                    new ObjectOutputStream(socket.getOutputStream()).writeObject(requestMessage);
                    System.out.println("|-- Sending ADD request to Replica " + otherReplicaId + " --|");
                } catch (IOException e) {
                    e.printStackTrace();
                    this.socket = outgoingSockets.get(otherReplicaId);
                }
            } else if(requestMessage.getLabel() == Labels.GET){
                try {
                    new ObjectOutputStream(socket.getOutputStream()).writeObject(requestMessage);
                    System.out.println("|-- Sending GET request to Replica " + otherReplicaId + " --|");
                } catch (IOException e) {
                    this.socket = outgoingSockets.get(otherReplicaId);
                }
            }
            try {
                Message responseMessage = (Message) new ObjectInputStream(socket.getInputStream()).readObject();
                System.out.println("<-- Request THREAD: Message received. -->");
                System.out.println(responseMessage.getLabel());
                if(responseMessage.getLabel() == Labels.ADD_OK || responseMessage.getLabel() == Labels.ADD_ALREADY_EXISTS){
                    System.out.println("|-- Received PUT_ACK from Replica " + otherReplicaId + " --|");
                    synchronized (confirmQueue){
                        //queue only keeps one response from each replica, discarding others
                        if(!confirmQueue.contains(responseMessage)){
                            confirmQueue.add(responseMessage);
                            confirmQueue.notify();
                        }
                    }
                    synchronized (notFinishedBuffer){
                        notFinishedBuffer.remove(responseMessage.getSenderId());
                    }
                } else if (responseMessage.getLabel() == Labels.GET_OK){
                    System.out.println("|-- Received GET_ACK from Replica " + otherReplicaId + " --|");
                    synchronized (responseQueue){
                        responseQueue.add(responseMessage);
                        responseQueue.notify();
                    }
                }
            } catch (IOException e) {
                this.socket = outgoingSockets.get(otherReplicaId);
            } catch (ClassNotFoundException e) {
                this.socket = outgoingSockets.get(otherReplicaId);
            }
        }
    }
    public void invokeRequest(Message m) {
        synchronized (bufferRequests){
            bufferRequests.add(m);
            bufferRequests.notify();
        }
    }

    public Socket getSocket() {
        return this.socket;
    }

}
