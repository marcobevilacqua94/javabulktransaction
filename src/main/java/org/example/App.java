package org.example;

import com.couchbase.client.core.cnc.events.transaction.TransactionCleanupAttemptEvent;
import com.couchbase.client.core.cnc.events.transaction.TransactionCleanupEndRunEvent;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.transaction.atr.ActiveTransactionRecordIds;
import com.couchbase.client.core.transaction.cleanup.CleanupRequest;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecord;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecordEntry;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecords;
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
import java.util.Optional;


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
        boolean upsert = args[8].equals("true");
        boolean firstClean = args[9].equals("true");
        try (Cluster cluster = Cluster.connect(
                args[0],
                ClusterOptions.clusterOptions(args[1], args[2])

                        .environment(env -> {
                            env.transactionsConfig(TransactionsConfig.builder()
                                    .timeout(Duration.ofSeconds(Integer.parseInt(args[5])))
                                    .build());
                            env.ioConfig().numKvConnections(Integer.parseInt(args[6]));
                        })
        )

        ) {
            if (firstClean) {
                ReactiveBucket bucket = cluster.bucket("test").reactive();
                bucket.waitUntilReady(Duration.ofSeconds(10)).block();
                for (String atr : ActiveTransactionRecordIds.allAtrs(1024)) {
                    Optional<ActiveTransactionRecords> optActs = ActiveTransactionRecord.getAtr(cluster.core(), CollectionIdentifier.fromDefault("test"), atr, Duration.ofMillis(5), null).onErrorComplete().block();
                    if (optActs == null) continue;
                    optActs.ifPresent(act -> {
                        System.out.println("Cleaning transaction record " + act.id());
                        for (ActiveTransactionRecordEntry entry : act.entries()) {
                            cluster.core().transactionsCleanup().getCleaner()
                                    .performCleanup(CleanupRequest
                                                    .fromAtrEntry(
                                                            CollectionIdentifier.fromDefault("test"),
                                                            entry
                                                    )
                                            , false, null).block();
                        }
                    });
                }
                cluster.environment().eventBus().subscribe(event -> {
                    if (event instanceof TransactionCleanupAttemptEvent || event instanceof TransactionCleanupEndRunEvent) {
                        System.out.println(event.description());
                    }
                });
                System.out.println("Waiting 5 secs...");
                Thread.sleep(5000);
            }


            cluster.core().transactionsCleanup().shutdown(Duration.ofSeconds(5));
            bulkTransactionReactive(jsonObject, cluster, args, true, upsert);
            System.out.println("Waiting 5 secs...");
            Thread.sleep(5000);

            System.out.println("Transaction Start");
            bulkTransactionReactive(jsonObject, cluster, args, false, upsert);

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    public static void bulkTransactionReactive(JsonObject jsonObject, Cluster cluster, String[] args, boolean warmup, boolean upsert) {
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

        int concurrency = Runtime.getRuntime().availableProcessors() * 2 * Integer.parseInt(args[7]);
        int parallelThreads = Runtime.getRuntime().availableProcessors() * Integer.parseInt(args[7]);

        TransactionResult result = cluster.reactive().transactions().run((ctx) -> {
                            Mono<Void> firstOp;
                            if (upsert) {
                                firstOp = ctx.get(coll, "1")
                                        .flatMap(doc -> ctx.replace(doc, jsonObject))
                                        .onErrorResume(DocumentNotFoundException.class, (er) -> ctx.insert(coll, "1", jsonObject)).then();
                            } else {
                                firstOp = ctx.insert(coll, "1", jsonObject).then();
                            }

                            Mono<Void> restOfOps = Flux.range(2, num - 1)
                                    .parallel(concurrency)
                                    .runOn(Schedulers.newBoundedElastic(parallelThreads, Integer.MAX_VALUE, "bounded"))
                                    .concatMap(
                                            docId -> {
                                                if (docId % 1000 == 0)
                                                    System.out.println("docId: " + docId);
                                                if (upsert) {
                                                    return ctx.get(coll, docId.toString()).
                                                            flatMap(doc -> ctx.replace(doc, jsonObject))
                                                            .onErrorResume(DocumentNotFoundException.class, (er) -> ctx.insert(coll, docId.toString(), jsonObject));
                                                } else {
                                                    return ctx.insert(coll, docId.toString(), jsonObject);
                                                }

                                            }
                                    ).sequential()
                                    .then();


                            return firstOp.then(restOfOps);

                        }, TransactionOptions.transactionOptions().
                                timeout(Duration.ofSeconds(Integer.parseInt(args[5])))
                )
                .doOnError(err ->
                        {
                            if (warmup)
                                System.out.println("Warmup transaction failed");
                            else
                                System.out.println("Transaction failed");
                            err.printStackTrace();
                        }
                )
                .block();


        long endTime = System.nanoTime();
        long duration = (endTime - startTime);

        if (warmup)
            System.out.println("Warmup transaction completed");
        else {
            System.out.println("Transaction completed");
            System.out.println("Transaction time: " + duration / 1000000000 + "s");
            System.out.println("Num of docs: " + num);
            System.out.println("Doc size: " + args[4] + "kb");
        }
        try (
                PrintWriter writer = new PrintWriter("logs_ExtParallelUnstaging.txt")) {
            result.logs().forEach(writer::println);
        } catch (
                FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
