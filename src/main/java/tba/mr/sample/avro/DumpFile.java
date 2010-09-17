package tba.mr.sample.avro;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectDatumReader;

public class DumpFile {
    public static void main(String argz[]) throws IOException {
        DatumReader<Record> reader = new ReflectDatumReader<Record>(Record.class); 
        DataFileReader<Record> dfr = new DataFileReader<Record>(new File("/tmp/records"), reader);

        System.out.format("version = %s, creator = %s\n", dfr.getMetaLong("version"), dfr.getMetaString("creator"));
        Iterator<Record> it = dfr.iterator();
        while (it.hasNext()) {
            System.out.println(it.next());
        }

    }

}
