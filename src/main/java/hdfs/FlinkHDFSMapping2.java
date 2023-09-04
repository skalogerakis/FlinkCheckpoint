package hdfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//https://markobigdata.com/2016/03/06/hdfs-files-stored-on-a-local-filesystem-linux/
//https://askubuntu.com/questions/359481/where-in-linux-file-system-can-i-see-files-of-hadoop-hdfs
public class FlinkHDFSMapping2 {

    public static void CommandExecutionStatus(int exitCode){
        if (exitCode == 0) {
            System.out.println("Success");
        } else {
            System.err.println("Failed. Exit code: " + exitCode);
        }
    }

    public static void main(String[] args) throws Exception {

        String hdfsFilePath =  "hdfs:/test";

        HashMap<String, HashMap<String, String>> complete = new HashMap<String, HashMap<String, String>>();


//        try {
            /*** HDFS FSCK CMD ***/

            // Build the hdfs info command to find all the blocks for a given file
            String[] hdfsInfoCmd = {"hdfs", "fsck", hdfsFilePath, "-files", "-blocks"};

            ProcessBuilder hdfs_pb = new ProcessBuilder(hdfsInfoCmd);
            Process hdfs_proc = hdfs_pb.start();

            // Wait for the command to complete and check status
            CommandExecutionStatus(hdfs_proc.waitFor());

            // Regex that matches the desired input
            String regexPattern = ".*(\\d+)\\. (BP-\\w.+).* len=(\\d*) Live_repl=(\\d*).*";
            Pattern pattern = Pattern.compile(regexPattern);

            BufferedReader hdfs_cmd_output = new BufferedReader(new InputStreamReader(hdfs_proc.getInputStream()));

            // read the output from the command
            String s = null;
            String key_path = null;
            HashMap<String, String> internal = new HashMap<String, String>();

            while ((s = hdfs_cmd_output.readLine()) != null)
            {
                System.out.println(s);
                if(s.startsWith("/")){
                    //Updates the file path that is used as key
                    key_path = s.split(" ")[0];
                } else if (s.isEmpty() && !internal.isEmpty()) {
                    // Update the hashmap, key as file_path
                    complete.put(key_path, internal);
                    internal = new HashMap<String, String>();
                }

                if(s.startsWith("Status")) break;   //We don't need extra information from that point on

                Matcher matcher = pattern.matcher(s);

                // When the pattern finds a match
                if (matcher.find()) {

                    // Your regex matched the line, take values from the regex groups
                    //String block_num = matcher.group(1); //The number of the block
                    String info_loc = matcher.group(2); //The information about location

                    String[] split_info = info_loc.split(":");
                    String block_pool_id = split_info[0];   //Block Pool Id (used in path)
                    String block_id = split_info[1];  //Specific Block

                    String block_id_path = block_id.substring(0, block_id.lastIndexOf("_"));
                    System.out.println("Path Info "+ block_pool_id + " Value Info " + block_id_path);

                    //Place all the blocks to the internal hashmap
                    internal.put(block_id_path, block_pool_id);



                }



            }

            System.out.println("YOOOOO");

//        } catch (IOException e) {
//            e.printStackTrace();
//        }


    }

}
