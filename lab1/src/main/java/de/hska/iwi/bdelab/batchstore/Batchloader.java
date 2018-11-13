package de.hska.iwi.bdelab.batchstore;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.StringTokenizer;
import java.util.stream.Stream;

import de.hska.iwi.bdelab.schema.Data;
import de.hska.iwi.bdelab.schema.PageView;
import manning.tap.DataPailStructure;

import org.apache.hadoop.fs.FileSystem;

import com.backtype.hadoop.pail.Pail;

public class Batchloader {

    // ...

    private Pail pTemp;
    private Pail pMaster;
    Pail<Data>.TypedRecordOutputStream ostemp;

	private void readPageviewsAsStream() {
        try {
            URI uri = Batchloader.class.getClassLoader().getResource("pageviews.txt").toURI();
            try (Stream<String> stream = Files.lines(Paths.get(uri))) {
                stream.forEach(line -> writeToPail(getDatafromString(line)));
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (URISyntaxException e1) {
            e1.printStackTrace();
        }
    }

    private Data getDatafromString(String pageview) {
        Data result = null;

        StringTokenizer tokenizer = new StringTokenizer(pageview);
        String ip = tokenizer.nextToken();
        String url = tokenizer.nextToken();
        String time = tokenizer.nextToken();

        System.out.println(ip + " " + url + " " + time);

        // ... create Data
        //create new Pave view
        PageView pv = new PageView();
        pv.set_ip(ip);
        pv.set_time(time);
        pv.set_url(url);
        try {
			ostemp.writeObject(pv);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
       
		
        
        
        

        return result;
    }

    private void writeToPail(Data data) {
        // ...
    }

    private void importPageviews() {

        // change this to "true" if you want to work
        // on the local machines' file system instead of hdfs
        boolean LOCAL = false;

        try {
            // set up filesystem
            FileSystem fs = FileUtils.getFs(LOCAL);

            // prepare temporary pail folder
            String newPath = FileUtils.prepareNewFactsPath(true, LOCAL);

            // master pail goes to permanent fact store
            String masterPath = FileUtils.prepareMasterFactsPath(false, LOCAL);

            pTemp = Pail.create(fs, newPath, new DataPailStructure());
            pMaster = Pail.create(fs, masterPath, new DataPailStructure());
            ostemp = pTemp.openWrite();
            
            // write facts to new pail
            readPageviewsAsStream();
            ostemp.close();
            // set up master pail and absorb new pail
            System.out.println("vor");
            pMaster.absorb(pTemp);
            System.out.println("danach");
            pMaster.consolidate();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Batchloader loader = new Batchloader();
        loader.importPageviews();
    }
}