package com.exercise.henry.io;

import com.exercise.henry.option.ParquetRewriterJobOption;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;

import java.io.IOException;

public class ParquetRecordConsumer implements Runnable {

    private ParquetWriter<Group> writer;

    public ParquetRecordConsumer(ParquetRewriterJobOption parquetRewriterJobOption, String parquetFilePath) {

        Configuration configuration = parquetRewriterJobOption.getConfiguration();
        try {
            GroupWriteSupport.setSchema(parquetRewriterJobOption.getSchema(), configuration);
            GroupWriteSupport groupWriteSupport = new GroupWriteSupport();
            writer = new ParquetWriter<>(new Path(parquetFilePath),
                    groupWriteSupport,
                    parquetRewriterJobOption.getRewriteDefaultCompressionCodecName(),
                    parquetRewriterJobOption.getRewriteDefaultBlockSize(),
                    parquetRewriterJobOption.getRewriteDefaultPageSize(),
                    parquetRewriterJobOption.getRewriteDefaultDictionaryPageSize(),
                    true,
                    false,
                    parquetRewriterJobOption.getRewriteParquetFileWriterVersion(),
                    configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void write(Group group) {
        try {
            writer.write(group);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {

    }
}
