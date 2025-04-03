package com.scalar.db.datapopulator;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Put;
import com.scalar.db.datapopulator.utils.RandomStringGenerator;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import com.scalar.db.service.TransactionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    public static void main(String[] args) throws IOException {
        Path scalarDbPropertiesFilePath = Paths.get(args[0]);
        int recordCount = Integer.parseInt(args[1]);

        TransactionFactory transactionFactory = TransactionFactory.create(scalarDbPropertiesFilePath);
        DistributedTransactionManager transactionManager = transactionFactory.getTransactionManager();

        int coreCount = Runtime.getRuntime().availableProcessors();
        int batchSize = 10;
        ExecutorService executorService = Executors.newFixedThreadPool(coreCount);
        CompletionService<Void> completionService = new ExecutorCompletionService<>(executorService);
        AtomicInteger insertedRecords = new AtomicInteger(0);
        int totalTasks = (int) Math.ceil((double) recordCount / batchSize);  // Ensure correct task tracking

        for (int i = 1; i <= recordCount; i += batchSize) {
            int finalI = i;
            completionService.submit(() -> {
                DistributedTransaction tx = null;
                try {
                    tx = transactionManager.start();
                    List<Put> puts = new ArrayList<>(batchSize);

                    for (int j = finalI; j < finalI + batchSize && j <= recordCount; j++) {
                        Key partitionKey = Key.ofBigInt("col1", j);
                        Key clusteringKey = Key.newBuilder()
                                .add(IntColumn.of("col2", j))
                                .add(BooleanColumn.of("col3", true))
                                .build();

                        Put put = Put.newBuilder()
                                .namespace("test")
                                .table("all_columns")
                                .partitionKey(partitionKey)
                                .clusteringKey(clusteringKey)
                                .value(FloatColumn.of("col4", Float.MAX_VALUE))
                                .value(DoubleColumn.of("col5", Double.MAX_VALUE))
                                .value(TextColumn.of("col6", RandomStringGenerator.generateRandomString(100)))
                                .value(BlobColumn.of("col7", RandomStringGenerator.generateRandomString(100).getBytes(StandardCharsets.UTF_8)))
                                .value(DateColumn.of("col8", LocalDate.of(2000, 1, 1)))
                                .value(TimeColumn.of("col9", LocalTime.of(1, 1, 1)))
                                .value(TimestampColumn.of("col10", LocalDateTime.of(2000, 1, 1, 1, 1)))
                                .value(TimestampTZColumn.of("col11", Instant.ofEpochMilli(1740041740)))
                                .build();

                        puts.add(put);
                    }

                    tx.put(puts);
                    tx.commit();

                    int currentInsertedRecords = insertedRecords.addAndGet(batchSize);
                    if (currentInsertedRecords % 10000 == 0) {
                        System.out.println("Total records inserted: " + currentInsertedRecords);
                    }
                } catch (TransactionException e) {
                    if (tx != null) {
                        try {
                            tx.abort();  // Ensure rollback on failure
                        } catch (TransactionException ex) {
                            ex.printStackTrace();
                        }
                    }
                    e.printStackTrace();
                }
                return null;
            });
        }

        executorService.shutdown();
        for (int i = 0; i < totalTasks; i++) {
            try {
                completionService.take(); // Process each completed task
            } catch (InterruptedException ignored) {
            }
        }

    }
}