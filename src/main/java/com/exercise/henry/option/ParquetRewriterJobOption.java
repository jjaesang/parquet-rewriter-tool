package com.exercise.henry.option;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import java.util.List;

@Builder
@Getter
@Setter
public class ParquetRewriterJobOption {

    private String tableFullName;
    private String keyColName;
    private List<String> keyColValueList;
    private List<String> targetColNameList;
    private MessageType schema;
    private int numThread;

    private Configuration configuration;
    private CompressionCodecName rewriteDefaultCompressionCodecName = CompressionCodecName.SNAPPY;
    private int rewriteDefaultBlockSize = ParquetWriter.DEFAULT_BLOCK_SIZE;
    private int rewriteDefaultPageSize = ParquetWriter.DEFAULT_PAGE_SIZE;
    private int rewriteDefaultDictionaryPageSize = (1024 * 2);
    private ParquetProperties.WriterVersion rewriteParquetFileWriterVersion = ParquetProperties.WriterVersion.PARQUET_1_0;

}
