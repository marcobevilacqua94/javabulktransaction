package org.example;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.*;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.transactions.TransactionResult;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import com.couchbase.client.java.transactions.config.TransactionsConfig;

import java.io.FileNotFoundException;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.PrintWriter;
import java.time.Duration;

public class App {

    private static String RandomString(int length) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int index = (int) (Math.random() * characters.length());
            sb.append(characters.charAt(index));
        }
        return sb.toString();
    }

    public static void main(String[] args) {

        System.out.println("Transaction Load Test");

        JsonObject jsonObject = JsonObject.create();

        jsonObject.put("body", RandomString(Integer.parseInt(args[4])));

        try (Cluster cluster = Cluster.connect(
                args[0],
                ClusterOptions.clusterOptions(args[1], args[2])

                        .environment(env -> {
                            env.transactionsConfig(TransactionsConfig.builder()
                                    .timeout(Duration.ofSeconds(Long.parseLong(args[5])))
                                    .durabilityLevel(DurabilityLevel.NONE)
                                    .build());
                            env.ioConfig().numKvConnections(64);
                        })
        )

        ) {
            bulkTransactionReactive(jsonObject, cluster, args, true);
            Thread.sleep(5000);

            bulkTransactionReactive(jsonObject, cluster, args, false);

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    public static void bulkTransactionReactive(JsonObject jsonObject, Cluster cluster, String[] args, boolean warmup) {
        long startTime = System.nanoTime();
        String collectionName = "test";
        int num;
        if (warmup) {
            collectionName = "warmup";
            num = 500;
        } else {
            num = Integer.parseInt(args[3]);
        }
        ReactiveBucket bucket = cluster.bucket("test").reactive();
        bucket.waitUntilReady(Duration.ofSeconds(10)).block();
        ReactiveCollection coll = bucket.scope("test").collection(collectionName);

        int concurrency = Runtime.getRuntime().availableProcessors() * 12;
        int parallelThreads = Runtime.getRuntime().availableProcessors() * 4;
        TransactionResult result = cluster.reactive().transactions().run((ctx) -> {

                    Mono<Void> firstOp = ctx.insert(coll, "1", jsonObject).then();

                    Mono<Void> restOfOps = Flux.range(2, num-1)
                            .parallel(concurrency)
                           // .runOn(Schedulers.newBoundedElastic(parallelThreads, Integer.MAX_VALUE, "bounded"))
                            .runOn(Schedulers.newParallel("parallel", parallelThreads))
                            .concatMap(
                                    docId -> {
                                        if (docId % 1000 == 0)
                                            System.out.println("docId: " + docId);
                                        return ctx.insert(coll, docId.toString(), jsonObject);
                                    }
                            ).sequential().then();


                    return firstOp.then(restOfOps);

                }, TransactionOptions.transactionOptions().timeout(Duration.ofSeconds(Long.parseLong(args[5])))
        ).doOnError(err -> {
            if(warmup)
                System.out.println("Warmup transaction failed");
            else
                System.out.println("Transaction failed");
        }).block();

        if(warmup)
            System.out.println("Warmup transaction completed");
        else {
            System.out.println("Transaction completed");
            long endTime = System.nanoTime();
            long duration = (endTime - startTime);
            System.out.println(duration / 1000000000 + "s");
        }
        try (PrintWriter writer = new PrintWriter("logs_ExtParallelUnstaging.txt")) {
            result.logs().forEach(writer::println);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
