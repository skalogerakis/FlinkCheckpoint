package hdfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.*;
import java.nio.file.*;
import java.io.*;
import java.nio.file.*;
//https://markobigdata.com/2016/03/06/hdfs-files-stored-on-a-local-filesystem-linux/
//https://askubuntu.com/questions/359481/where-in-linux-file-system-can-i-see-files-of-hadoop-hdfs
public class CatCmd {

    public static void CommandExecutionStatus(int exitCode){
        if (exitCode == 0) {
            System.out.println("Success");
        } else {
            System.err.println("Failed. Exit code: " + exitCode);
        }
    }

    public static void main(String[] args) throws Exception {

        /*  Elapsed Time Cat cmd
            1 Mb -> ~ 150ms
            200Mb -> ~ 1150ms
            400Mb -> ~ 2150ms
            200Mb + 200Mb -> ~2000ms
         */

        String localOutputFilePath = "/home/skalogerakis/file_test.txt";
        //String hdfsFilePath =  "hdfs:/sample.txt";


        //Init the cat cmd
        List<String> concat_cmd = new ArrayList<String>(){

//            {add("cat");}
        };

        Instant before = Instant.now();

        concat_cmd.add("bash");
        concat_cmd.add("-c");
        concat_cmd.add("/home/skalogerakis/Documents/Workspace/FlinkCheckpoint/cat_script.sh /home/skalogerakis/TestFiles/200M.txt,/home/skalogerakis/TestFiles/200_2M.txt,/home/skalogerakis/TestFiles/200_3M.txt /home/skalogerakis/file_out.txt");
//        concat_cmd.add("cat");
//        concat_cmd.add("cat /home/skalogerakis/TestFiles/200M.txt /home/skalogerakis/TestFiles/400M.txt > /home/skalogerakis/file_test2.txt");
//        concat_cmd.add("/home/skalogerakis/TestFiles/200M.txt");
//        concat_cmd.add("/home/skalogerakis/TestFiles/200M_2.txt");
//        concat_cmd.add("/home/skalogerakis/TestFiles/400M.txt");

//        concat_cmd.add("cat /home/skalogerakis/TestFiles/200M.txt /home/skalogerakis/TestFiles/200_2M.txt > /home/skalogerakis/file_test2.txt");



        /*** CONCAT CMD ***/
        // Finish with the concat process after finding all the blocks
        ProcessBuilder concat_pb = new ProcessBuilder(concat_cmd);
//        concat_pb.redirectOutput(ProcessBuilder.Redirect.to(new File(localOutputFilePath)));
        Process concat_proc = concat_pb.start();

        CommandExecutionStatus(concat_proc.waitFor());


        Instant after = Instant.now();

        System.out.println("CAT Elapsed Time: " + Duration.between(before, after).toMillis());





//        List<String> concat_cmd = new ArrayList<String>(){
//
//            {add("cat");}
//        };
//
//        concat_cmd.add("/home/skalogerakis/TestFiles/200M.txt");
//        concat_cmd.add("/home/skalogerakis/TestFiles/200M_2.txt");
//        concat_cmd.add("/home/skalogerakis/TestFiles/400M.txt");
//
//        Instant before_new = Instant.now();
//        /*** CONCAT CMD ***/
//
//        // Finish with the concat process after finding all the blocks
//        ProcessBuilder concat_pb = new ProcessBuilder(concat_cmd);
//        //concat_pb.redirectOutput(ProcessBuilder.Redirect.to(new File(localOutputFilePath)));
//        Process concat_proc = concat_pb.start();
//
//
//
//        InputStream isFromCat = concat_proc.getInputStream();
//        OutputStream osCombinedFile = new FileOutputStream(new File(localOutputFilePath));
//
//        byte[] buffer = new byte[8 * 1024];
//        int read = 0;
//        while((read = isFromCat.read(buffer)) != -1) {
//            osCombinedFile.write(buffer, 0, read);
//        }
//
//        CommandExecutionStatus(concat_proc.waitFor());
//
//        Instant after_new = Instant.now();
//
//        System.out.println("Byte Elapsed Time: " + Duration.between(before_new, after_new).toMillis());


    }

}
