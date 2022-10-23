package init;

import Server.Server;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

public class ServerStart {
    public static void main(String[] args){
        System.out.println(Arrays.asList(args));
        if (args.length != 2) {
            System.out.println("Usage: java init.ServerStart <file> <replicaID>");
            System.exit(1);
        } else {
            try {
                File file = new File(args[0]);
                if (file.exists()) {
                    int nServers = countNumberOfLinesInFile(file);
                    if(nServers >= Integer.parseInt(args[1])){
                        new Server(Integer.parseInt(args[1]), file, nServers);
                    } else {
                        System.out.println("Replica ID is greater than number of servers");
                        System.exit(1);
                    }
                } else {
                    System.out.println("File does not exist");
                    System.exit(1);
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Error: " + e.getMessage());
                System.exit(1);
            }
        }
    }
    private static int countNumberOfLinesInFile(File file) {
        int lines = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            while (br.readLine() != null) {
                lines++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lines;
    }
}
