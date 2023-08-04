package main;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FlinkHDFSMapping {

    public static void CommandExecutionStatus(int exitCode){
        if (exitCode == 0) {
            System.out.println("Success");
        } else {
            System.err.println("Failed. Exit code: " + exitCode);
        }
    }

    public static void main(String[] args) throws Exception {

        String outputFile = "/home/skalogerakis/file_out4.txt";
//        Files.createDirectories(Paths.get(outputFile));
//        Files.createDirectories(restoreFilePath);
//        String hdfsFilePath = remoteFileHandle.toString(); // The HDFS path to the file
//        String localOutputFilePath = restoreFilePath.toString(); // The local path where the file will be moved

//        String test =  "hdfs:/flink-checkpoints/34cd14c9665c3eb53e41c3e4c596f837/shared/8d31cf70-ce80-436a-bd51-f458de465bc3";
        String test =  "hdfs:/sample.txt";

        List<String> commands = new ArrayList<String>(){{add("cat");}};



        try {
            // Build the command to execute
//            String command = "hdfs fsck " + test + " -files -blocks"; //-locations does not seem to be required

//            String command = "hdfs fsck " + test + " -files -blocks"; //-locations does not seem to be required
            String[] command = {"hdfs", "fsck", test, "-files", "-blocks"};
//            Runtime rt = Runtime.getRuntime();
//
//            // Execute the command
//            Process process = rt.exec(command);


            ProcessBuilder pb = new ProcessBuilder(command);

            Process process = pb.start();
            // Wait for the command to complete
            CommandExecutionStatus(process.waitFor());


            BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));

//            String regexPattern = ".* len=(\\d*) Live_repl=(\\d*).*";
            // Regex that matches the desired input
            String regexPattern = ".*(\\d+)\\. (BP-\\w.+).* len=(\\d*) Live_repl=(\\d*).*";
            Pattern pattern = Pattern.compile(regexPattern);

            // read the output from the command
            String s = null;
            while ((s = stdInput.readLine()) != null)
            {
                if(s.startsWith("Status")) break;   //We don't need extra information from that point on
                System.out.println(s);

                Matcher matcher = pattern.matcher(s);

                if (matcher.find()) {
                    //We need this command
//                    find /tmp/hadoop-fs-tmp/current/BP-798034145-127.0.0.1-1690967214498/current/finalized -name 'blk_1073741832*'
                    // Your regex matched the line, take values from the regex groups
                    String block_num = matcher.group(1); //The number of the block
                    String info_loc = matcher.group(2); //The information about location

                    System.out.println("Value 1: " + block_num + " Value 2: " + info_loc);


                    String[] split_info = info_loc.split(":");

                    String path_info = split_info[0];
                    String block_info = split_info[1];
                    String sub_block_info = block_info.substring(0, block_info.lastIndexOf("_"));
                    System.out.println("Path Info "+ path_info + " Value Info " + sub_block_info);

//                    String fin_path = "/tmp/hadoop-fs-tmp/current/" + path_info + "/current/finalized -name " + sub_block_info + "";
//
//                    String fin_path = "/tmp/hadoop-fs-tmp/current/" + path_info + "/current/finalized -name '" + sub_block_info + "*'";
                    String fin_path = "/tmp/hadoop-fs-tmp/current/" + path_info + "/current/finalized";

                    System.out.println(fin_path);

                    //TODO use find with the fin path to find where the required files are located


//                    String find_command = "find " + fin_path;
                    String[] find_command = {"find", fin_path, "-name", sub_block_info};
                    System.out.println("Find command " + find_command);
                    // Execute the command

                    ProcessBuilder pb3 = new ProcessBuilder(find_command);

//                    Process process3 = rt.exec(find_command);
                    Process process3 = pb3.start();

                    // Wait for the command to complete
                    CommandExecutionStatus(process3.waitFor());



                    BufferedReader reader = new BufferedReader(new InputStreamReader(process3.getInputStream()));
                    String line;

                    while ((line=reader.readLine())!=null)
                    {
                        System.out.println(line);
                        commands.add(line);
                    }

                }



            }



            // creating the process
            ProcessBuilder pb2 = new ProcessBuilder(commands);
            pb2.redirectOutput(ProcessBuilder.Redirect.to(new File(outputFile)));
            // starting the process
            Process process2 = pb2.start();

            CommandExecutionStatus(process2.waitFor());

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

    }

}
