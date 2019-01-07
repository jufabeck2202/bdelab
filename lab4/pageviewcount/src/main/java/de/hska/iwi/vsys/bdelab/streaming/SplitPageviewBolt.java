package de.hska.iwi.vsys.bdelab.streaming;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

public class SplitPageviewBolt extends NoisyBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
    	System.out.println(getIDs() + " executes tuple: " + tuple);

		String[] data = tuple.getString(4).split("\\s");
		String url = data[1];
		int timestamp = Integer.parseInt(data[2]);

		String time = getTimestamp(timestamp);
		String normalizedUrl = normalizeUrl(url);
		
		String timestampWithUrl = time + " " + normalizedUrl;

		collector.emit(new Values(timestampWithUrl));
    }

    private String getTimestamp(long timeInMS) {
		SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.YYYY - HH");
		Date date = new Date();
		date.setTime(timeInMS *1000L);
		System.out.println(date);
		String timestamp = sdf.format(date);
		return timestamp;
	}

	private String normalizeUrl(String url) {
    	try {
			//rohe Url wird aus functionalCall 0  ausgelesen, verwenden der URL lib für normalisierung
			URL rawUrl = new URL(url);
			
			// Bestandteile der URL werden ermittelt und zu einer neuen URL zusammengesetzt 
			// getProtocol -> protokoll der URL
			//getHost -> host der URl
			//
			URL normalizedUrl = new URL(rawUrl.getProtocol(), rawUrl.getHost(), rawUrl.getPath());
			// Neues Tupel zur Menge hinzufügen, 
			//toExternalForm damit url zu einem String wird.
		
			return normalizedUrl.toExternalForm();
		} catch (MalformedURLException e) {
			//Fehler behandlung
			e.printStackTrace();
			return "Ein Fehler ist aufgetreten";
		}
	}

	@Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
