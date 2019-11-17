package com.exercise.henry.io;

import com.exercise.henry.convert.NullValueConverter;
import com.exercise.henry.option.ParquetRewriterJobOption;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;


import java.io.IOException;

public class ParquetRecordProducer implements Runnable {

    private NullValueConverter nullValueConverter;
    private ParquetReader<Group> reader;
    private Configuration conf;

    public ParquetRecordProducer(ParquetRewriterJobOption parquetRewriterJobOption, String parquetFilePath) throws IOException {

        this.reader = ParquetReader.builder(new GroupReadSupport(), new Path(parquetFilePath))
                .withConf(parquetRewriterJobOption.getConfiguration())
                .build();

        this.nullValueConverter = new NullValueConverter(parquetRewriterJobOption);

    }

    public void producer() throws IOException {
        Group group;
        while ((group = (Group) reader.read()) != null) {
            //    System.out.println("-----------------------------------------------------------");
            //   PrintGroupUtil.printGroup(group);
            //    PrintGroupUtil.printFieldCount(group);
            //    System.out.println("***********************************************************");
            // PrintGroupUtil.printGroup(nullValueConverter.convertGroup(group));
            //PrintGroupUtil.printFieldCount(nullValueConverter.convertGroup(group));
            //   System.out.println("-----------------------------------------------------------");
            //   System.out.println();
            nullValueConverter.convertGroup(group);
        }
    }

    @Override
    public void run() {

    }
}
