package com.exercise.henry.io;

import com.exercise.henry.option.ParquetRewriterJobOption;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.util.Arrays;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ParquetRecordProducerTest {

    private ParquetRewriterJobOption parquetRewriterJobOption;
    private String parquetFilePath;

    @BeforeAll
    public void setOption() {
        this.parquetRewriterJobOption = ParquetRewriterJobOption.builder()
                .tableFullName("test")
                .keyColName("id")
                .keyColValueList(Arrays.asList("2;3;4;5".split(";")))
                .targetColNameList(Arrays.asList("first_name;last_name".split(";")))
                .configuration(new Configuration())
                .build();

        this.parquetFilePath = "userdata1.parquet";

        try {
            ParquetMetadata parquetMetadata = ParquetFileReader
                    .readFooter(parquetRewriterJobOption.getConfiguration(),
                            new Path(parquetFilePath),
                            ParquetMetadataConverter.NO_FILTER);
            parquetRewriterJobOption.setSchema(parquetMetadata.getFileMetaData().getSchema());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    void producer() throws IOException {

        ParquetRecordProducer parquetRecordProducer = new ParquetRecordProducer(
                parquetRewriterJobOption,
                parquetFilePath
        );

        parquetRecordProducer.producer();


    }
}