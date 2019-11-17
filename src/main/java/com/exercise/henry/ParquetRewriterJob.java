package com.exercise.henry;

import com.exercise.henry.io.ParquetRecordConsumer;
import com.exercise.henry.io.ParquetRecordProducer;
import com.exercise.henry.option.ParquetRewriterJobOption;
import com.exercise.henry.option.ParquetRewriterJobOptionParser;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ParquetRewriterJob {

    private ParquetRewriterJobOption parquetRewriterJobOption;

    private ExecutorService producerExecutor;
    private ExecutorService consumerExecutor;

    private void init(String[] args) {

        ParquetRewriterJobOptionParser parquetRewriterJobOptionParser = new ParquetRewriterJobOptionParser();
        parquetRewriterJobOption = parquetRewriterJobOptionParser.parse(args);

        this.producerExecutor = Executors.newFixedThreadPool(parquetRewriterJobOption.getNumThread());
        this.consumerExecutor = Executors.newFixedThreadPool(parquetRewriterJobOption.getNumThread());
    }

    private void startProducer() {

        try {
            producerExecutor.execute(new ParquetRecordProducer(parquetRewriterJobOption, ""));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void stopProducer() {

        try {
            producerExecutor.shutdown();
            producerExecutor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            log.info("interrupted exception occurred");
        }
    }

    private void startConsumer() {

        consumerExecutor.execute(new ParquetRecordConsumer(parquetRewriterJobOption, ""));
    }

    private void stopConsumer() {

        try {
            consumerExecutor.shutdown();
            consumerExecutor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            log.info("interrupted exception occurred");
        }

    }

    private void run() {

        startProducer();
        startConsumer();

        stopProducer();
        stopConsumer();

    }

    public static void main(String[] args) {

        ParquetRewriterJob parquetRewriterJob = new ParquetRewriterJob();
        parquetRewriterJob.init(args);
        parquetRewriterJob.run();

    }
}
