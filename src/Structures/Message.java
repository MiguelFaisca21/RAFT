package Structures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Set;

public class Message implements Serializable {
    private int senderId;
    private int receiverId;
    private long label;
    private ArrayList<String> data;

    public Message(int senderId, int receiverId, long label, ArrayList<String> data) {
        this.senderId = senderId;
        this.receiverId = receiverId;
        this.label = label;
        this.data = data;
    }

    public int getSenderId() {
        return senderId;
    }

    public int getReceiverId() {
        return receiverId;
    }

    public long getLabel() {
        return label;
    }

    public ArrayList<String> getData() {
        return data;
    }
}
