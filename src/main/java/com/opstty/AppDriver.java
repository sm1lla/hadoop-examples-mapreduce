package com.opstty;

import com.opstty.job.*;
import com.opstty.mapper.HeightSortMapper;
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
            programDriver.addClass("maxheight", KindHeight.class,
                    "A map/reduce program that computes the maximum height per kind of tree.");
            programDriver.addClass("heightsort", HeightSort.class,
                    "A map/reduce program that that sorts the heights of trees.");
            programDriver.addClass("oldesttree", OldestTree.class,
                    "A map/reduce program that that sorts the heights of trees.");
            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
