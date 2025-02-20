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
import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
public class FlinkJsonBenchmark {

    private JsonRowDataDeserializationSchema deserializer;
    private List<String> jsonLines;

    @Setup
    public void setup() throws Exception {
        // Define the schema using RowType
        RowType rowType = RowType.of(
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

        var bidsType =                 new RowType(
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

        // Create JSON deserializer with correct constructor
        deserializer = new JsonRowDataDeserializationSchema(
                rowType,
                TypeInformation.of(RowData.class),
                false,  // failOnMissingField
                false,  // ignoreParseErrors
                TimestampFormat.ISO_8601
        );

        deserializer.open(null);

        jsonLines = Files.readAllLines(Paths.get("../data/nexmark/nexmark.json"), StandardCharsets.UTF_8);
        jsonLines = jsonLines.subList(0, 1024);
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
