package tba.mr.sample.avro;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;

public class GenerateFile {
    private static final int SIZE = 10000;
    public static Record[] records = new Record[SIZE]; 
    static {
        for (int i=0; i<SIZE; i++) {
            records[i] = new Record(Integer.toString(i), new Date(), new Date());
        }
    }
    public static void main(String argz[]) throws IOException {
        Schema schema = ReflectData.get().getSchema(Record.class);

        //seems redundant here
        DatumWriter writer = new ReflectDatumWriter(Record.class);
        DataFileWriter file = new DataFileWriter(writer);
        file.setMeta("version", 1);
        file.setMeta("creator", "ThinkBigAnalytics");
        file.setCodec(CodecFactory.deflateCodec(5));
        file.create(schema, new File("/tmp/records"));
        for (Record record : records) {
            file.append(record);
        }
        file.close();
    }
}
