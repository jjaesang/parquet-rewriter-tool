package com.exercise.henry.option;

import org.apache.commons.cli.*;

import java.util.Arrays;

public class ParquetRewriterJobOptionParser {

    public ParquetRewriterJobOption parse(String[] args) {

        Options options = makeOptions(args);

        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("help", options);


        return ParquetRewriterJobOption.builder()
                .tableFullName(commandLine.getOptionValue("table-name"))
                .keyColName(commandLine.getOptionValue("key-col-name"))
                .keyColValueList(Arrays.asList(commandLine.getOptionValue("key-col-value").split(";")))
                .targetColNameList(Arrays.asList(commandLine.getOptionValue("target-col-name").split(";")))
                .numThread(Integer.parseInt(commandLine.getOptionValue("num-thread")))
                .build();

    }

    private Options makeOptions(String[] args) {

        Options options = new Options();
        options.addOption(Option.builder().longOpt("table-name")
                .desc("the full name of target table [ example : DB.TABLE ]")
                .hasArg()
                .required(true)
                .build());

        options.addOption(Option.builder().longOpt("key-col-name")
                .desc("key column name of target table [ example :  cstno ]")
                .hasArg()
                .required(true)
                .build());

        options.addOption(Option.builder().longOpt("key-col-value")
                .desc("key column value list of target table, delimiter is ';' [ example : 12345;45678;01235 ]")
                .hasArg()
                .required(true)
                .build());

        options.addOption(Option.builder().longOpt("target-col-name")
                .desc("deleted column name list of target table, delimiter is ';'")
                .hasArg()
                .required(true)
                .build());

        options.addOption(Option.builder().longOpt("num-thread")
                .desc("multi-thread")
                .hasArg()
                .required(true)
                .build());

        options.addOption(Option.builder().longOpt("compression")
                .desc("compression codec name of new parquet file, when not input impala default value")
                .hasArg()
                .build());

        options.addOption(Option.builder().longOpt("block-size")
                .desc("blocksize of new parquet file, when not input impala default value")
                .hasArg()
                .build());

        options.addOption(Option.builder().longOpt("page-size")
                .desc("pagesize of new parquet file, when not input impala default value")
                .hasArg()
                .build());

        options.addOption(Option.builder().longOpt("dict-page-size")
                .desc("dictionary page Size of new parquet file, when not input impala default value")
                .hasArg()
                .build());

        options.addOption(Option.builder().longOpt("writer-version")
                .desc("new parquet file writer version, when not input impala default value [ example : PARQUET_1_0, PARQUET_2_0]")
                .hasArg()
                .build());

        return options;
    }
}
