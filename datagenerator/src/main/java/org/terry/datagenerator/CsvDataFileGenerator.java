package org.terry.datagenerator;

import org.terry.common.util.Utils;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class CsvDataFileGenerator {
    private final int fieldCount;
    private final int fieldValueLength;
    private final int recordCount;
    private final String outputPath;
    private final int startRecord;

    private final double valueRatio;

    private static final String EMPTY = "";
    private static final String KEY_PREFIX = "user";
    private static final String ID = "record_key";

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("invalid args");
            System.exit(1);
        }
        int idx = 0;
        int fieldCount = 10;
        int fieldValueLength = 10;
        int recordCount = 10;
        int start = 0;
        double ratio = 1;
        String outputPath = null;
        while (idx < args.length) {
            String arg = args[idx];
            if ("-fieldCount".equals(arg)) {
                fieldCount = Integer.parseInt(args[idx + 1]);
            } else if ("-fieldValueLength".equals(arg)) {
                fieldValueLength = Integer.parseInt(args[idx + 1]);
            } else if ("-recordCount".equals(arg)) {
                recordCount = Integer.parseInt(args[idx + 1]);
            } else if ("-output".equals(arg)) {
                outputPath = args[idx + 1];
            } else if ("-start".equals(arg)) {
                start = Integer.parseInt(args[idx + 1]);
            } else if ("-ratio".equals(arg)) {
                ratio = Double.parseDouble(args[idx + 1]);
                if (ratio <= 0 || ratio > 1) {
                    System.err.println("ratio should between (0, 1]");
                    System.exit(1);
                }
            }
            idx += 2;
        }

        if (outputPath == null) {
            System.err.println("please input -output");
            System.exit(1);
        }
        long startTime = System.currentTimeMillis();
        CsvDataFileGenerator generator = new CsvDataFileGenerator(fieldCount, fieldValueLength, recordCount, outputPath, start, ratio);
        System.out.println("csv file generating...");
        generator.doGenerate();
        System.out.println("csv file generate done. time cost: " + (System.currentTimeMillis() - startTime));
    }

    public CsvDataFileGenerator(int fieldCount, int fieldValueLength, int recordCount, String outputPath, int startRecord, double valueRatio) {
        this.fieldCount = fieldCount;
        this.fieldValueLength = fieldValueLength;
        this.recordCount = recordCount;
        this.outputPath = outputPath;
        this.startRecord = startRecord;
        this.valueRatio = valueRatio;
    }

    public void doGenerate() {
        List<String> fieldNames = new ArrayList<>(fieldCount + 1);
        List<String> fieldHeaders = new ArrayList<>(fieldCount + 1);
        fieldNames.add(ID);
        fieldHeaders.add(ID + ".string()");
        for (int i = 0; i < fieldCount; i++) {
            String field = "field" + i;
            fieldNames.add(field);
            fieldHeaders.add(field + ".string()");

        }

        try (BufferedWriter w = new BufferedWriter(new FileWriter(outputPath))){
            w.write(String.join(",", fieldHeaders));
            w.newLine();

            int percent = 0;
            for (int i = 0; i < recordCount; i++) {
                List<String> vals = new ArrayList<>(fieldCount + 1);
                String key = buildKey(startRecord + i);
                vals.add(key);
                for (int j = 0; j < fieldNames.size(); j++) {
                    String field = fieldNames.get(j);
                    String val = EMPTY;
                    if (j >= 10) {
                        double r = ThreadLocalRandom.current().nextDouble();
                        if (r < valueRatio) {
                            val = buildValue(key, field);
                        }
                    } else {
                        val = buildValue(key, field);
                    }
                    vals.add(val);
                }
                w.write(String.join(",", vals));
                w.newLine();

                int progress = ((i + 1) * 100) / recordCount;
                if (progress > percent) {
                    percent = progress;
                    System.out.printf("%s processing %d%%......\n", LocalDateTime.now().toString(), percent);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private String buildValue(String key, String fieldName) {
        StringBuilder sb = new StringBuilder(fieldValueLength);
        sb.append(Utils.hash((key + fieldName).hashCode()));
        while (sb.length() < fieldValueLength) {
            sb.append(':');
            sb.append(Utils.hash(sb.toString().hashCode()));
        }
        sb.setLength(fieldValueLength);
        return sb.toString();
    }

    private String buildKey(int num) {
        String value = Long.toString(Utils.hash(num));
        return KEY_PREFIX + value;
    }
}
