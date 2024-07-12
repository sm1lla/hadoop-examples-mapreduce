package com.opstty;

import com.opstty.job.DistrictList;
import com.opstty.job.KindCount;
import com.opstty.job.SpeciesList;
import com.opstty.job.WordCount;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("districtlist", DistrictList.class,
                    "A map/reduce program that lists all districs with trees.");
            programDriver.addClass("species", SpeciesList.class,
                    "A map/reduce program that lists all species of trees.");
            programDriver.addClass("kinds", KindCount.class,
                    "A map/reduce program that counts the number of all occuring species of trees.");
            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
