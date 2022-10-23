package Server;

import Structures.Labels;
import Structures.Message;
import Threads.ReadAndReplyThread;
import Threads.RequestAndReceiveThread;
import javafx.util.Pair;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

public class Server {
    private final int port;
    private final int replicaId;
    private final int nServers;
    private final HashMap<Integer, Pair<String, Integer>> replicasInfo;
    private final HashMap<Integer, RequestAndReceiveThread> serverRequestThreads = new HashMap<>();
    private final HashMap<Integer, ReadAndReplyThread> serverReplyThreads = new HashMap<>();
    private HashMap<Integer, Socket> incomingSockets = new HashMap<>();
    private HashMap<Integer, Socket> outgoingSockets = new HashMap<>();
    private Set<String> dataSet = new HashSet<>();
    private Queue<Message> confirmQueue = new LinkedList<>();
    private Queue<Message> responseQueue = new LinkedList<>();
    private HashMap<Integer,Message> notFinishedBuffer = new HashMap<>();
    private HashMap<Integer, Pair<String, Integer>> createListOfAddressPort(File file) {
        HashMap<Integer, Pair<String, Integer>> addressPort = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            int i = 0;
            while ((line = br.readLine()) != null) {
                String[] addressPortArray = line.split(":");
                addressPort.put(i, new Pair<>(addressPortArray[0], Integer.parseInt(addressPortArray[1])));
                i++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return addressPort;
    }
    private static boolean isNumeric(String str) {
        try {
            Integer.parseInt(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    public Server(int replicaId, File file, int nServers) {
        this.replicaId = replicaId;
        this.replicasInfo = createListOfAddressPort(file);
        this.port = replicasInfo.get(replicaId).getValue();
        this.nServers = nServers;
        new Thread(() -> receiveConnectionsFromReplicas()).start();
        new Thread(() -> openFirstConnectionsToReplicas()).start();
        receiveInputFromTerminal();
        //TODO: use shutdown hook in case of control c

    }
    private void receiveConnectionsFromReplicas() {
        try {
            ServerSocket serverSocket = new ServerSocket(this.port);
            ThreadFactory threadFactory = Executors.defaultThreadFactory();
            ThreadPoolExecutor executorPool = (ThreadPoolExecutor) Executors.newFixedThreadPool((this.nServers*2)-2, threadFactory);
            while (true) {
                listenForConnections(serverSocket, executorPool);
            }
        } catch (IOException e) {
            System.out.println("IO Error: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            System.out.println("Class not found: " + e.getMessage());
        }
    }
    private void listenForConnections(ServerSocket serverSocket, ThreadPoolExecutor executorPool) throws IOException, ClassNotFoundException {
        Socket socket = serverSocket.accept();
        Message receivedHandshake = (Message) new ObjectInputStream(socket.getInputStream()).readObject();
        if (this.incomingSockets.get(receivedHandshake.getSenderId()) == null) {
            this.incomingSockets.put(receivedHandshake.getSenderId(), socket);
            while (this.outgoingSockets.get(receivedHandshake.getSenderId()) == null) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    System.out.println("Thread sleep Error: " + e.getMessage());
                }
            }
            RequestAndReceiveThread requestAndReceiveThread = new RequestAndReceiveThread(this.replicaId, receivedHandshake.getSenderId(), this.outgoingSockets, this.confirmQueue, this.responseQueue, this.notFinishedBuffer);
            ReadAndReplyThread readAndReplyThread = new ReadAndReplyThread(this.replicaId, receivedHandshake.getSenderId(), this.incomingSockets, this.outgoingSockets, this.replicasInfo, this.dataSet);
            serverRequestThreads.put(receivedHandshake.getSenderId(), requestAndReceiveThread);
            serverReplyThreads.put(receivedHandshake.getSenderId(), readAndReplyThread);
            executorPool.execute(requestAndReceiveThread);
            executorPool.execute(readAndReplyThread);
            System.out.println(this.notFinishedBuffer);

            //in case of an add request not being finished, do the request again to prevent replica blockage
            if(this.notFinishedBuffer.containsKey(receivedHandshake.getSenderId())) {
                requestAndReceiveThread.invokeRequest(this.notFinishedBuffer.get(receivedHandshake.getSenderId()));
            }

            //update replica dataset with the dataset from other replicas
            for(int i = 1; i < receivedHandshake.getData().size(); i++){
                System.out.println(receivedHandshake.getData().get(i));
            }
            //dataSet = new HashSet<>(Collections.singleton(receivedHandshake.getData().get(1)));

            System.out.println("INCOMING SOCKET CREATED: Replica with ID " + this.replicaId + " received connection from Replica with ID " + receivedHandshake.getSenderId() + " (" + socket.getInetAddress() + ":" + receivedHandshake.getData().get(0) + ")");
        } else {
            this.incomingSockets.put(receivedHandshake.getSenderId(), socket);
            System.out.println("INCOMING SOCKET UPDATED: Replica with ID " + this.replicaId + " received connection from Replica with ID " + receivedHandshake.getSenderId() + " (" + socket.getInetAddress() + ":" + receivedHandshake.getData().get(0) + ")");
        }
    }
    private void openFirstConnectionsToReplicas() {
        while(true){
            for (int i = 0; i < this.replicasInfo.size(); i++) {
                if (i != this.replicaId && this.outgoingSockets.get(i) == null) {
                    try {
                        Socket newSocket = openConnectionToReplica(this.replicaId, i, this.replicasInfo, this.outgoingSockets, this.dataSet);
                        System.out.println("OUTGOING SOCKET CREATED: Replica with ID " + replicaId + " connected to Replica with ID " + i + " (" + newSocket.getInetAddress() + ":" + newSocket.getPort() +")");
                    } catch (IOException e) {
                        //System.out.println("OUTGOING: Replica with ID " + replicaId + " failed to connect to Replica with ID " + i);
                    }
                }
            }
        }
    }
    public static Socket openConnectionToReplica(int replicaId, int otherReplicaId, HashMap<Integer, Pair<String, Integer>> replicasInfo, HashMap<Integer, Socket> outgoingSockets, Set<String> dataSet) throws IOException {
        Socket socket = new Socket(replicasInfo.get(otherReplicaId).getKey(), replicasInfo.get(otherReplicaId).getValue());
        outgoingSockets.put(otherReplicaId, socket);
        ArrayList<String> handshake = new ArrayList<>();
        handshake.add(replicasInfo.get(replicaId).getValue().toString());
        for(String s : dataSet) {
            handshake.add(s);
        }
        new ObjectOutputStream(socket.getOutputStream()).writeObject(new Message(replicaId, otherReplicaId, Labels.HANDSHAKE, handshake));
        return socket;
    }
    private void receiveInputFromTerminal() {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
        try {
            while (true) {
                String[] command = bufferedReader.readLine().split(" ");
                if (command[0].equals("get") && isNumeric(command[1]) && command.length == 2) {
                    boolean result = invoke(Labels.GET, Integer.parseInt(command[1]));
                    if (result) {
                        System.out.println("GET: Replica with ID " + replicaId + " successfully invoked GET operation on replica " + command[1]);
                    } else {
                        System.out.println("GET: Replica with ID " + replicaId + " failed to invoke GET operation on replica " + command[1]);
                    }
                } else if (command[0].equals("add") && command.length == 2){
                    Boolean result = invokeQuorum(Labels.ADD, command[1]);
                    if (result) {
                        System.out.println("ADD: Replica with ID " + replicaId + " successfully invoked ADD operation of value " + command[1]);
                    } else {
                        System.out.println("ADD: Replica with ID " + replicaId + " failed to invoke ADD operation of value " + command[1]);
                    }
                } else if (command[0].equals("threads") && command.length == 1){
                    System.out.println("----------------------------------------------");
                    System.out.println("Number of request Threads:"+ serverRequestThreads.size());
                    for (int i = 1; i <= serverRequestThreads.size()+1; i++) {
                        if (i != replicaId) {
                            System.out.println(serverRequestThreads.get(i).getSocket());
                        }
                    }
                    System.out.println("----------------------------------------------");
                    System.out.println("Number of reply Threads:"+ serverReplyThreads.size());
                    for (int i = 1; i <= serverReplyThreads.size()+1; i++) {
                        if (i != replicaId) {
                            System.out.println(serverReplyThreads.get(i).getSocket());
                        }
                    }
                    System.out.println("----------------------------------------------");
                } else if (command[0].equals("exit") && command.length == 1) {
                    System.out.println("Terminating server...");
                    for(int i = 0; i < this.outgoingSockets.size(); i++) {
                        if (i != this.replicaId) {
                            this.outgoingSockets.get(i).close();
                        }
                    }
                    for(int i = 0; i < this.incomingSockets.size(); i++) {
                        if (i != this.replicaId) {
                            this.incomingSockets.get(i).close();
                        }
                    }
                    System.exit(0);
                } else if (command[0].equals("help") && command.length == 1) {
                    System.out.println("Available commands:");
                    System.out.println("get <replicaID>");
                    System.out.println("add <string>");
                    System.out.println("threads");
                    System.out.println("exit");
                } else {
                    System.out.println("Invalid command. Try 'help'");
                }
            }
        } catch (NumberFormatException e) {
            System.out.println("Error while reading request: try get <replicaID> where replicaID is an integer.");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    private boolean invoke(int label, int replicaId) {
        boolean result = false;
        if (label == Labels.GET && replicaId >= 0 && replicaId <= this.replicasInfo.size()) {
            if(this.replicaId == replicaId){
                synchronized (this.dataSet) {
                    if(!this.dataSet.isEmpty()){
                        System.out.println("Dataset content of replica "+this.replicaId+" : "+this.dataSet);
                    } else {
                        System.out.println("Dataset content of replica "+this.replicaId+" : empty");
                    }
                }
               result = true;
            } else {
                if(this.outgoingSockets.get(replicaId) != null) {
                    Message getRequest = new Message(this.replicaId, replicaId, Labels.GET, null);
                    serverRequestThreads.get(replicaId).invokeRequest(getRequest);
                    synchronized (this.responseQueue) {
                        while (this.responseQueue.isEmpty()) {
                            try {
                                this.responseQueue.wait();
                            } catch (InterruptedException e) {
                                System.out.println("Runtime Exception: " + e.getMessage());
                            }
                        }
                        Message getResponse = this.responseQueue.poll();
                        if (getResponse != null && getResponse.getLabel() == Labels.GET_OK) {
                            if(!this.dataSet.isEmpty()){
                                System.out.println("Dataset content of replica "+replicaId+" : "+getResponse.getData());
                            } else {
                                System.out.println("Dataset content of replica "+replicaId+" : empty");
                            }
                            result = true;
                        }
                    }
                } else {
                    System.out.println("Replica with ID " + replicaId + " is not connected. Try to connect that replica before trying to get data from it.");
                }
            }
        }
        return result;
    }
    private boolean invokeQuorum(int label, String value) {
        boolean result = false;
        if (!dataSet.contains(value)) {
            if (label == Labels.ADD) {
                for (int i = 0; i <= serverRequestThreads.size(); i++) {
                    if (i != this.replicaId) {
                        System.out.println(serverRequestThreads.get(i).getSocket());
                        ArrayList<String> data = new ArrayList<>();
                        data.add(value);
                        Message addRequest = new Message(this.replicaId, i, Labels.ADD, data);
                        serverRequestThreads.get(i).invokeRequest(addRequest);
                    }
                }

                for (int i = 0; i < nServers; i++) {
                    if (i != this.replicaId) {
                        ArrayList<String> data = new ArrayList<>();
                        data.add(value);
                        Message addRequest = new Message(this.replicaId, i, Labels.ADD, data);
                        synchronized (notFinishedBuffer) {
                            notFinishedBuffer.put(i, addRequest);
                        }
                    }
                }

                synchronized (confirmQueue) {
                    //number of confirmations is always nServers / 2, since we have nServers servers and we need nServers / 2 confirmations (excluding the server itself)
                    int nConfirmationsExceptItself = nServers / 2;
                    while (confirmQueue.size() < nConfirmationsExceptItself) {
                        try {
                            confirmQueue.wait();
                        } catch (InterruptedException e) {
                            System.out.println("Runtime Exception: " + e.getMessage());
                        }
                    }
                    confirmQueue.clear();
                    dataSet.add(value);
                    notFinishedBuffer.clear();
                    System.out.println("Added " + value + " to the dataset.");
                }
                result = true;
            }
        }
        return result;
    }
}
