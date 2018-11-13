package de.hska.iwi.bdelab.batchstore;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.StringTokenizer;
import java.util.stream.Stream;
import de.hska.iwi.bdelab.schema.Data;
import de.hska.iwi.bdelab.schema.DataUnit;
import de.hska.iwi.bdelab.schema.PageViewEdge;
import de.hska.iwi.bdelab.schema.Pedigree;
import de.hska.iwi.bdelab.schema.UserID;
import de.hska.iwi.bdelab.schema.Website;
import manning.tap.DataPailStructure;

import org.apache.hadoop.fs.FileSystem;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.PailFormatFactory;

public class Batchloader {

    // ...


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
        

        StringTokenizer tokenizer = new StringTokenizer(pageview);
        String ip = tokenizer.nextToken();
        String url = tokenizer.nextToken();
        String time = tokenizer.nextToken();

        System.out.println(ip + " " + url + " " + time);

        // ... create Data
        
        //create new user
        UserID userID = new UserID();
		userID.set_user_id(ip);
		
		//create new Page
		Website site = new Website();
		site.set_url(url);
		
		//connect user to page 
		PageViewEdge view = new PageViewEdge();
		view.set_time(time);
		view.set_page(site);
		view.set_user(userID);
		
		Pedigree pedigree = new Pedigree( Integer.valueOf(time));
		Data result = new Data();
		DataUnit unit = new DataUnit();
	
		
		unit.set_page_view(view);
		result.set_pedigree(pedigree);
		result.set_dataunit(unit);

        

        return result;
    }

    private void writeToPail(Data data) {
        // ...
    	 try {
 			ostemp.writeObject(data);
 		} catch (IOException e) {
 			// TODO Auto-generated catch block
 			e.printStackTrace();
 		}
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

            //Pail pTemp = Pail.create(fs, newPath, new DataPailStructure());
            //Pail pMaster = Pail.create(fs, masterPath, new DataPailStructure());
            Pail<Data> pTemp = Pail.create(fs, newPath, PailFormatFactory.getDefaultCopy().setStructure(new DataPailStructure()));
			Pail<Data> pMaster = Pail.create(fs, masterPath, PailFormatFactory.getDefaultCopy().setStructure(new DataPailStructure()));
            
            ostemp = pTemp.openWrite();
            
            // write facts to new pail
            readPageviewsAsStream();
            
            ostemp.close();
            // set up master pail and absorb new pail
            pMaster.absorb(pTemp);
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