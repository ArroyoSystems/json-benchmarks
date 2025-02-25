package dev.arroyo.benchmarks;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
public class FlinkJsonBenchmark {

    @Param({"nexmark", "bids", "logs", "tweets"})
    private String dataset;

    @Param({"false"})
    private String pretty;

    private JsonRowDataDeserializationSchema deserializer;
    private List<String> jsonLines;

    static RowType nexmarkSchema() {
        return RowType.of(
                new RowType(
                        java.util.Arrays.asList(
                                new RowType.RowField("id", new IntType()),
                                new RowType.RowField("name", new VarCharType()),
                                new RowType.RowField("email_address", new VarCharType()),
                                new RowType.RowField("credit_card", new VarCharType()),
                                new RowType.RowField("city", new VarCharType()),
                                new RowType.RowField("state", new VarCharType()),
                                new RowType.RowField("datetime", new VarCharType()),
                                new RowType.RowField("extra", new VarCharType())
                        )
                ),
                new RowType(
                        java.util.Arrays.asList(
                                new RowType.RowField("id", new IntType()),
                                new RowType.RowField("item_name", new VarCharType()),
                                new RowType.RowField("description", new VarCharType()),
                                new RowType.RowField("initial_bid", new IntType()),
                                new RowType.RowField("reserve", new IntType()),
                                new RowType.RowField("datetime", new VarCharType()),
                                new RowType.RowField("expires", new VarCharType()),
                                new RowType.RowField("seller", new IntType()),
                                new RowType.RowField("category", new IntType()),
                                new RowType.RowField("extra", new VarCharType())
                        )
                ),
                new RowType(
                        java.util.Arrays.asList(
                                new RowType.RowField("auction", new IntType()),
                                new RowType.RowField("bidder", new IntType()),
                                new RowType.RowField("price", new IntType()),
                                new RowType.RowField("channel", new VarCharType()),
                                new RowType.RowField("url", new VarCharType()),
                                new RowType.RowField("datetime", new VarCharType()),
                                new RowType.RowField("extra", new VarCharType())
                        )
                )
        );
    }

    static RowType bidSchema() {
        return new RowType(
                java.util.Arrays.asList(
                        new RowType.RowField("auction", new IntType()),
                        new RowType.RowField("bidder", new IntType()),
                        new RowType.RowField("price", new IntType()),
                        new RowType.RowField("channel", new VarCharType()),
                        new RowType.RowField("url", new VarCharType()),
                        new RowType.RowField("datetime", new VarCharType()),
                        new RowType.RowField("extra", new VarCharType())
                )
        );
    }

    public static RowType tweetSchema() {
        return new RowType(Arrays.asList(
                new RowType.RowField("metadata", new RowType(
                        java.util.Arrays.asList(
                                new RowType.RowField("result_type", new VarCharType()),
                                new RowType.RowField("iso_language_code", new VarCharType())
                        )
                )),
                new RowType.RowField("created_at", new VarCharType()),
                new RowType.RowField("id", new BigIntType()),
                new RowType.RowField("id_str", new VarCharType()),
                new RowType.RowField("text", new VarCharType()),
                new RowType.RowField("source", new VarCharType()),
                new RowType.RowField("truncated", new BooleanType()),
                new RowType.RowField("in_reply_to_status_id", new BigIntType(true)),
                new RowType.RowField("in_reply_to_status_id_str", new VarCharType()),
                new RowType.RowField("in_reply_to_user_id", new BigIntType(true)),
                new RowType.RowField("in_reply_to_user_id_str", new VarCharType()),
                new RowType.RowField("in_reply_to_screen_name", new VarCharType()),
                new RowType.RowField("user", new RowType(
                        java.util.Arrays.asList(
                                new RowType.RowField("id", new BigIntType()),
                                new RowType.RowField("id_str", new VarCharType()),
                                new RowType.RowField("name", new VarCharType()),
                                new RowType.RowField("screen_name", new VarCharType()),
                                new RowType.RowField("location", new VarCharType()),
                                new RowType.RowField("description", new VarCharType()),
                                new RowType.RowField("url", new VarCharType()),
                                new RowType.RowField("followers_count", new IntType()),
                                new RowType.RowField("friends_count", new IntType()),
                                new RowType.RowField("listed_count", new IntType()),
                                new RowType.RowField("created_at", new VarCharType()),
                                new RowType.RowField("favourites_count", new IntType())
                        )
                )),
                new RowType.RowField("retweet_count", new IntType()),
                new RowType.RowField("favorite_count", new IntType()),
                new RowType.RowField("lang", new VarCharType())
        ));
    }

    public static RowType logSchema() {
        return new RowType(Arrays.asList(
                new RowType.RowField("ip", new VarCharType()),
                new RowType.RowField("identity", new VarCharType()),
                new RowType.RowField("user_id", new VarCharType()),
                new RowType.RowField("timestamp", new VarCharType()), // Nanoseconds precision
                new RowType.RowField("request", new VarCharType()),
                new RowType.RowField("status_code", new IntType(false)), // UInt32 maps to Int32 in Flink
                new RowType.RowField("size", new IntType(false)),        // UInt32 maps to Int32
                new RowType.RowField("referer", new VarCharType()),
                new RowType.RowField("user_agent", new VarCharType())
        ));
    }


    @Setup
    public void setup() throws Exception {
        // Define the schema using RowType

        RowType schema;
        String filename;
        switch (dataset) {
            case "tweets": {
                schema = tweetSchema();
                filename = "../data/tweets/tweets";
                break;
            }
            case "nexmark": {
                schema = nexmarkSchema();
                filename = "../data/nexmark/nexmark";
                break;
            }
            case "bids": {
                schema = bidSchema();
                filename = "../data/nexmark/bids";
                break;
            }
            case "logs": {
                schema = logSchema();
                filename = "../data/logs/logs";
                break;
            }
            default: {
                throw new Exception("invalid dataset " + dataset);
            }
        }

        // Create JSON deserializer with correct constructor
        deserializer = new JsonRowDataDeserializationSchema(
                schema,
                TypeInformation.of(RowData.class),
                false,  // failOnMissingField
                false,  // ignoreParseErrors
                TimestampFormat.ISO_8601
        );

        deserializer.open(null);

        if (pretty.equals("true")) {
            filename += "_pretty";
        }

        jsonLines = Files.readAllLines(Paths.get(filename + ".json"), StandardCharsets.UTF_8);
    }

    @Benchmark
    public List<RowData> deserialize() throws IOException {
        ArrayList<RowData> outputs = new ArrayList<>(jsonLines.size());

        for (String l : jsonLines) {
            outputs.add(deserializer.deserialize(l.getBytes()));
        }

        return outputs;
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(FlinkJsonBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}
