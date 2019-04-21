package com.bluehonour.spark.utils;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

/**
 * 生成日期时间
 */
public class GenerateTimeUtils {
    private static String timeid;
    private static String year = "2017";
//    private static String[] month = {"01","02","03","04","05","06","07","08","09","10","11","12"};
    private static String month = "01";
//    private static String[] day = {"01","02","03","04","05","06","07","08","09","10",
//            "11","12","13","14","15","16","17","18","19",
//            "20","21","22","23", "24","25","26","27","28","29","30","31"};
    private static String[] day = {"08","09"};
    private static String[] hour = {"00","01","02","03","04","05","06","07","08","09","10",
            "11","12","13","14","15","16","17","18","19",
            "20","21","22","23"};
    private static String[] minute = {"00","01","02","03","04","05","06","07","08","09","10",
            "11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26",
            "27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42",
            "43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59",
    };

    public static void main(String[] args) {

        FileOutputStream fos = null;
        PrintStream ps = null;
        try {
            fos = new FileOutputStream("/home/liushuai/data/time.dat");
            ps = new PrintStream(fos);

            int count = 0;
//            for(int i=0; i<month.length; i++){
                for(int j=0; j<day.length; j++){
                    for(int k=0; k<hour.length; k++){
                        for(int l=0; l<minute.length; l++){
                            timeid = new String(year+month+day[j]+hour[k]+minute[l]);
                            String line = timeid+"\t"+year+"\t"+month+"\t"+day[j]+"\t"+hour[k]+"\t"+minute[l];
                            System.out.println(line);
                            ps.println(line);
                            count++;
                        }
                    }
//                }
            }

            System.out.println(count);


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                if(ps!=null){
                    ps.close();
                }
                if(fos!=null){
                    fos.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }
}
