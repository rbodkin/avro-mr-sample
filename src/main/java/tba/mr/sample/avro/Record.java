package tba.mr.sample.avro;

import java.util.Date;

public class Record {

    public Record() {
        
    }
    
    public Record(String service, Date start, Date end) {        
        this.service = service;
        startTimestamp = start.getTime();
        endTimestamp = end.getTime();
    }

    public String service;
    
    public long startTimestamp;
    public long endTimestamp;
    
    // Avro silently ignores these date fields - well it emits a hole in the output but
    // doesn't read/write
    transient public Date start;
    
    transient public Date end;
    
    public String toString() {
        start = new Date(startTimestamp);
        end = new Date(endTimestamp);
        return String.format("Record: %s %s %s", service, start, end);
    }
    
}
