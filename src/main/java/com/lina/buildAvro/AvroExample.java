package com.lina.buildAvro;

import com.lina.customAvro.ReversedLogicalType;
import com.lina.query.QueryRecord;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class AvroExample {
    public static void main(String[] args) {
        LogicalTypes.register(ReversedLogicalType.REVERSED_LOGICAL_TYPE_NAME, new LogicalTypes.LogicalTypeFactory() {
            private final LogicalType reversedLogicalType = new ReversedLogicalType();
            @Override
            public LogicalType fromSchema(Schema schema) {
                return reversedLogicalType;
            }
        });
        //LogicalTypes.register("reversed", new ReversedLogicalType.TypeFactory());
        QueryRecord queryRecord = QueryRecord.newBuilder()
                .setQueryId("first")
                .setQueryAuthor("Anybody")
                .build();

        final DatumWriter<QueryRecord> datumWriter = new SpecificDatumWriter<>(QueryRecord.class);

        try (DataFileWriter<QueryRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(queryRecord.getSchema(), new File("query.avro"));
            dataFileWriter.append(queryRecord);
            System.out.println("successfully wrote query.avro");
            System.out.println("*****************************");
        } catch (IOException e){
            e.printStackTrace();
        }

        // read it from a file
        final File file = new File("query.avro");
        final DatumReader<QueryRecord> datumReader = new SpecificDatumReader<>(QueryRecord.class);
        final DataFileReader<QueryRecord> dataFileReader;
        try {
            System.out.println("Reading our specific record");
            System.out.println("*****************************");
            dataFileReader = new DataFileReader<>(file, datumReader);
            while (dataFileReader.hasNext()) {
                QueryRecord query = dataFileReader.next();
                System.out.println(query.toString());
                System.out.println("Query ID        : " + query.getQueryId());
                System.out.println("Query Author    : " + query.getQueryAuthor());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
