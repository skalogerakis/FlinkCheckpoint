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

        String localOutputFilePath = "/home/skalogerakis/file_out6.txt";
        String hdfsFilePath =  "hdfs:/sample.txt";
//        Files.createDirectories(Paths.get(outputFile));
//        Files.createDirectories(restoreFilePath);
//        String hdfsFilePath = remoteFileHandle.toString(); // The HDFS path to the file
//        String localOutputFilePath = restoreFilePath.toString(); // The local path where the file will be moved

        //Init the cat cmd
        List<String> concat_cmd = new ArrayList<String>(){{add("cat");}};

        try {
            /*** HDFS FSCK CMD ***/

            // Build the hdfs info command to find all the blocks for a given file
            String[] hdfsInfoCmd = {"hdfs", "fsck", hdfsFilePath, "-files", "-blocks"};

            ProcessBuilder hdfs_pb = new ProcessBuilder(hdfsInfoCmd);
            Process hdfs_proc = hdfs_pb.start();

            // Wait for the command to complete and check status
            CommandExecutionStatus(hdfs_proc.waitFor());


            // String regexPattern = ".* len=(\\d*) Live_repl=(\\d*).*";
            // Regex that matches the desired input
            String regexPattern = ".*(\\d+)\\. (BP-\\w.+).* len=(\\d*) Live_repl=(\\d*).*";
            Pattern pattern = Pattern.compile(regexPattern);

            BufferedReader hdfs_cmd_output = new BufferedReader(new InputStreamReader(hdfs_proc.getInputStream()));

            // read the output from the command
            String s = null;
            while ((s = hdfs_cmd_output.readLine()) != null)
            {
                if(s.startsWith("Status")) break;   //We don't need extra information from that point on
                //System.out.println(s);

                Matcher matcher = pattern.matcher(s);

                // When the pattern finds a match
                if (matcher.find()) {

                    // Your regex matched the line, take values from the regex groups
                    // String block_num = matcher.group(1); //The number of the block
                    String info_loc = matcher.group(2); //The information about location
                    //System.out.println("Value 2: " + info_loc);


                    String[] split_info = info_loc.split(":");
                    String block_pool_id = split_info[0];   //Block Pool Id (used in path)
                    String block_id = split_info[1];  //Specific Block

                    String block_id_path = block_id.substring(0, block_id.lastIndexOf("_"));
                    //System.out.println("Path Info "+ block_pool_id + " Value Info " + block_id_path);

                    /*** FIND CMD ***/
                    //THIS is the goal command. However we don't seem to need *
                    // find /tmp/hadoop-fs-tmp/current/BP-798034145-127.0.0.1-1690967214498/current/finalized -name 'blk_1073741832*'
                    //String fin_path = "/tmp/hadoop-fs-tmp/current/" + path_info + "/current/finalized -name '" + sub_block_info + "*'";
                    String find_search_path = "/tmp/hadoop-fs-tmp/current/" + block_pool_id + "/current/finalized";


                    String[] find_command = {"find", find_search_path, "-name", block_id_path};
                    // Execute the command

                    ProcessBuilder find_pb = new ProcessBuilder(find_command);
                    Process find_proc = find_pb.start();

                    // Wait for the command to complete
                    CommandExecutionStatus(find_proc.waitFor());


                    BufferedReader find_cmd_output = new BufferedReader(new InputStreamReader(find_proc.getInputStream()));

                    String line = null;
                    //We are expecting to find one path only. In case no path Error
                    if ((line=find_cmd_output.readLine())!=null)
                        concat_cmd.add(line);
                    else
                        System.out.println("ERROR -> Could not find " + find_search_path + ", with BlockID: " + block_id_path);


                }



            }


            /*** CONCAT CMD ***/
            // Finish with the concat process after finding all the blocks
            ProcessBuilder concat_pb = new ProcessBuilder(concat_cmd);
            concat_pb.redirectOutput(ProcessBuilder.Redirect.to(new File(localOutputFilePath)));
            Process concat_proc = concat_pb.start();

            CommandExecutionStatus(concat_proc.waitFor());

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

    }

}
