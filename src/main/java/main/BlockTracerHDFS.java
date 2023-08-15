package main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BlockTracerHDFS {

    // Dictionary to store the total size by ip node
    static Map<String, Long> hdfsStats = new HashMap<>();
    public static void resultPrinter() {
//        int totalSize = 0;

        // Find the total size for sanity reasons
//        for (int total : hdfsStats.values()) {
//            totalSize += total;
//        }

//        System.out.println("Total Data Size = " + totalSize);

        System.out.println("Sorted by value size");
        // Sorting the map by value size
        hdfsStats.entrySet()
                .stream()
                .sorted(Map.Entry.<String, Long>comparingByValue())
                .forEach(entry -> {
                    String ip = entry.getKey();
                    long size = entry.getValue();
//                    double percentage = ((double) size / totalSize) * 100;
                    System.out.println("IP: " + ip + "\tSIZE: " + size);
                });
    }

    // In case the dictionary is empty return empty string otherwise the IP containing the highest block capacity
    public static String Ip_Finder() {

        if (hdfsStats.isEmpty()) return "";

        // Sorting the map by value size and get the first IP available
        return hdfsStats.entrySet().stream()
                .max(Comparator.comparing(Map.Entry::getValue)).get().getKey();


    }



    public static void main(String[] args) {

        String path = "hdfs:/flink-checkpoints/a585af6b0b1aed38c4facfb9df3daf7c";

        try {
            //HDFS command to execute
            String[] hdfsInfoCmd = {"hdfs", "fsck", path, "-files", "-blocks", "-locations"};

            ProcessBuilder processBuilder = new ProcessBuilder(hdfsInfoCmd);
            Process process = processBuilder.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

            String line;
            while ((line = reader.readLine()) != null) {

                // Only lines containing DatanodeInfoWithStorage are valuable
                if (line.contains("DatanodeInfoWithStorage")) {

                    // Regex to get the length and replication factor of a HDFS block
                    String regexLenRep = ".* len=(\\d*) Live_repl=(\\d*).*";
                    Pattern patternLenRep = Pattern.compile(regexLenRep);
                    Matcher matcherLenRep = patternLenRep.matcher(line);

                    if (matcherLenRep.find()) {
                        /** BLOCK SIZE AND REPLICATION FACTOR **/
                        int blockSize = Integer.parseInt(matcherLenRep.group(1));
                        int replicationFactor = Integer.parseInt(matcherLenRep.group(2));
                        //System.out.println("Block Size: " + blockSize + "\tRF: " + replicationFactor);

                        /*
                            Regex to get all the ips on which a HDFS block is present. In the case of more than one replicas
                            DatanodeInfoWithStorage appears multiple times with the desired IP
                         */
                        String regexIps = ".*" + ".*DatanodeInfoWithStorage\\[([\\w.]*)".repeat(replicationFactor);

                        Pattern patternIps = Pattern.compile(regexIps);
                        Matcher matcherIps = patternIps.matcher(line);

                        if (matcherIps.find()) {

                            //Add all the replicas to the IP dictionary
                            for (int i = 1; i <= replicationFactor; i++) {
                                String ip = matcherIps.group(i);
                                //System.out.println(ip);

                                // Calculate per ip size and get the corresponding load
                                hdfsStats.put(ip, hdfsStats.getOrDefault(ip, 0L) + blockSize);
                            }

                        }
                    }
                }
            }

            //resultPrinter();
            String hostingIP = Ip_Finder();
            System.out.println(hostingIP);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}