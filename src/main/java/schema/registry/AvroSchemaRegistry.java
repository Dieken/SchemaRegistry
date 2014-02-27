package schema.registry;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroSchemaRegistry extends AbstractSchemaRegistry {

    public static String TYPE = "avro";
    private static Logger logger = LoggerFactory.getLogger(AvroSchemaRegistry.class);
    private Map<String, Schema> avroSchemas = new HashMap<>();

    public AvroSchemaRegistry(Map<String, SchemaInfo> schemas, File rootDirectory)
            throws IOException {
        super(schemas, rootDirectory);

        Schema.Parser parser = new Schema.Parser();
        for (Map.Entry<String, SchemaInfo> e : schemas.entrySet()) {
            String id = e.getKey();
            SchemaInfo info = e.getValue();

            if (TYPE.equals(info.getType())) {
                Schema schema = parser.parse(new File(rootDirectory, id + "/" + info.getFilename()));
                avroSchemas.put(id, schema);

                logger.debug("parsed avro schema {}({})", id, schema.getFullName());
            }
        }
    }

    @Override
    public void serialize(String id, String messageName, InputStream in, OutputStream out, Map<String, String[]> parameters) throws IOException {
        Schema schema = getSchema(id);

        if (wantsDataFile(parameters)) {
            String codec = ParameterUtil.getParameter(parameters, "avro.codec");
            if (codec == null) {
                codec = "null";
            }

            serializeDataFile(schema, in, out,
                    CodecFactory.fromString(codec));
        } else {
            convertRecord(schema,
                    DecoderFactory.get().jsonDecoder(schema, in),
                    EncoderFactory.get().binaryEncoder(out, null));
        }
    }

    @Override
    public void deserialize(String id, String messageName, InputStream in, OutputStream out, Map<String, String[]> parameters) throws IOException {
        Schema schema = getSchema(id);

        if (wantsDataFile(parameters)) {
            // only use the writer schema embedded in file
            deserializeDataFile(in, out);
        } else {
            convertRecord(schema,
                    DecoderFactory.get().binaryDecoder(in, null),
                    EncoderFactory.get().jsonEncoder(schema, out));
        }
    }

    private void convertRecord(Schema schema, Decoder decoder, Encoder encoder) throws IOException {
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        GenericRecord record = reader.read(null, decoder);

        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        writer.write(record, encoder);
        encoder.flush();
    }

    private void serializeDataFile(Schema schema, InputStream in, OutputStream out, CodecFactory c) throws IOException {
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);

        try (DataFileWriter<GenericRecord> fout = new DataFileWriter<>(writer).setCodec(c).create(schema, out)) {
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, in);
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);

            GenericRecord record = null;
            while (true) {
                try {
                    record = reader.read(record, decoder);
                } catch (EOFException ex) {
                    break;
                }

                fout.append(record);
            }
        }
    }

    private void deserializeDataFile(InputStream in, OutputStream out) throws IOException {
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>();

        try (DataFileStream<GenericRecord> fin = new DataFileStream<>(in, reader)) {
            Schema schema = fin.getSchema();
            reader.setSchema(schema);

            Encoder encoder = EncoderFactory.get().jsonEncoder(schema, out);
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);

            GenericRecord record = null;
            while (fin.hasNext()) {
                record = fin.next(record);
                writer.write(record, encoder);
            }

            encoder.flush();
        }
    }

    private Schema getSchema(String id) {
        Schema schema = avroSchemas.get(id);
        if (schema == null) {
            throw new IllegalArgumentException("unknown schema ID");
        }

        return schema;
    }

    private boolean wantsDataFile(Map<String, String[]> parameters) {
        return "file".equalsIgnoreCase(ParameterUtil.getParameter(parameters, "avro.payload"));
    }
}
