package de.hska.iwi.bdelab.batchqueries;

import org.apache.hadoop.fs.FileSystem;
import com.backtype.hadoop.pail.Pail;
import com.twitter.maple.tap.StdoutTap;
import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;
import jcascalog.Api;
import jcascalog.Subquery;

import de.hska.iwi.bdelab.batchstore.FileUtils;
import de.hska.iwi.bdelab.schema2.Data;
import de.hska.iwi.bdelab.schema2.DataUnit;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

public class SimpleBatchWorkflow extends QueryBase {

	// Move newData to master while preserving the newDataPail to keep receiving
	// incoming data
	@SuppressWarnings("rawtypes")
	public static void ingest(Pail masterPail, Pail newDataPail) throws Exception {
		FileSystem fs = FileUtils.getFs(false);

		// create snapshot from newPail
		Pail snapshotPail = newDataPail.snapshot(FileUtils.getTmpPath(fs, "newDataSnapshot", true, false));

		// clone the snapshot
		Pail snapshotCopy = snapshotPail.createEmptyMimic(FileUtils.getTmpPath(fs, "newDataSnapshotCopy", true, false));
		snapshotCopy.copyAppend(snapshotPail);

		// absorb clone into master (the clone will be gone)
		masterPail.absorb(snapshotCopy);

		// clear snapshot from newPail
		newDataPail.deleteSnapshot(snapshotPail);

		// now the snapshot could be deleted as well
		snapshotPail.clear();
	}

	@SuppressWarnings("rawtypes")
	public static void normalizeURLs() throws IOException {
		Tap masterDataset = dataTap(FileUtils.prepareMasterFactsPath(false, false));
		Tap outTap = dataTap(FileUtils.prepareResultsPath("normalized-by-url", true, false));

		// "_" is a special field that will ignore the values for that field.
		Api.execute(outTap,
				new Subquery("?result").predicate(masterDataset, "_", "?raw").predicate(new ExtractPageViewFields(), "?raw")
						.out("?url", "?time").predicate(new NormalizeUrl(), "?url").out("?normalized-url")
						.predicate(new UpdateUrl(), "?normalized-url", "?raw").out("?result")

		);
	}

	public static class UpdateUrl extends CascalogFunction {

		@Override
		public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
			Data data = (Data) functionCall.getArguments().getObject(1);//Datum lesen
			String url = (String) functionCall.getArguments().getObject(0);//normalisierte URL lesen
			data.get_dataunit().get_pageview().get_page().set_url(url);//URL im Datum updaten
			functionCall.getOutputCollector().add(new Tuple(data));//Datum 
			System.out.println("URL:"+url);
		}

	}

	public static class NormalizeUrl extends CascalogFunction {

		@Override
		public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
			try {
				URL rawUrl = new URL(functionCall.getArguments().getString(0));// rohe Url wird aus functionalCall
																				// gelesen
				URL normalizedUrl = new URL(rawUrl.getProtocol(), rawUrl.getHost(), rawUrl.getPath());// Bestandteile
																										// der URL
																										// werden
																										// ermittelt und
																										// zu einer
																										// neuen URL
																										// zusammengesetzt
				functionCall.getOutputCollector().add(new Tuple(normalizedUrl.toExternalForm()));// Neues Tupel zur
																									// Menge hinzufügen
				System.out.println("NORM"+normalizedUrl);
			} catch (MalformedURLException e) {
				e.printStackTrace();
				System.out.println("Ein Fehler ist aufgetreten");
			}
		}

	}

	@SuppressWarnings("serial")
	public static class ExtractPageViewFields extends CascalogFunction {
		@SuppressWarnings("rawtypes")
		public void operate(FlowProcess process, FunctionCall call) {
			Data data = ((Data) call.getArguments().getObject(0)).deepCopy();
			DataUnit du = data.get_dataunit();
			if (du.getSetField() == DataUnit._Fields.PAGEVIEW) {
				String url = du.get_pageview().get_page().get_url();
				int time = data.get_pedigree().get_true_as_of_secs();
				call.getOutputCollector().add(new Tuple(url, time));
			}
		}
	}

	@SuppressWarnings("serial")
	public static class ToHour extends CascalogFunction {
		@SuppressWarnings("rawtypes")
		public void operate(FlowProcess process, FunctionCall call) {
			int time = call.getArguments().getInteger(0);
			int hour = time / (60 * 60);
			call.getOutputCollector().add(new Tuple(hour));
		}
	}

	@SuppressWarnings("serial")
	public static class ToGranularityBuckets extends CascalogFunction {
		@SuppressWarnings("rawtypes")
		public void operate(FlowProcess process, FunctionCall call) {
			int hourBucket = call.getArguments().getInteger(0);
			int dayBucket = hourBucket / 24;
			int weekBucket = dayBucket / 7;
			int monthBucket = dayBucket / 28;
			call.getOutputCollector().add(new Tuple("h", hourBucket));
			call.getOutputCollector().add(new Tuple("d", dayBucket));
			call.getOutputCollector().add(new Tuple("w", weekBucket));
			call.getOutputCollector().add(new Tuple("m", monthBucket));
		}
	}

	@SuppressWarnings("rawtypes")
	public static void viewsPerHour() throws IOException {
		Tap normalizedByUrl = dataTap(FileUtils.prepareResultsPath("normalized-by-url", false, false));

		// first query part aggregates views by url and hour
		Subquery hourlyRollup = new Subquery("?url", "?hour-bucket", "?hour-count")
				.predicate(normalizedByUrl, "_", "?fact").predicate(new ExtractPageViewFields(), "?fact")
				.out("?url", "?time").predicate(new ToHour(), "?time").out("?hour-bucket")
				.predicate(new jcascalog.op.Count(), "?hour-count")
				.predicate(new Debug(), "?url", "?hour-bucket", "?hour-count").out("?one");

		// sink into stdout in absence of serving layer db
		Api.execute(new StdoutTap(),
				new Subquery("?url", "?granularity", "?bucket", "?bucket-count")
						.predicate(hourlyRollup, "?url", "?hour-bucket", "?hour-count")
						.predicate(new ToGranularityBuckets(), "?hour-bucket").out("?granularity", "?bucket")
						.predicate(new jcascalog.op.Sum(), "?hour-count").out("?bucket-count"));
	}

	@SuppressWarnings("rawtypes")
	public static void batchWorkflow() throws Exception {
		// Hadoop konfigurieren
		setApplicationConf();

		// Init batch store pails
		Pail masterPail = new Pail(FileUtils.prepareMasterFactsPath(false, false));
		Pail newDataPail = new Pail(FileUtils.prepareNewFactsPath(false, false));

		// Start workflow
		ingest(masterPail, newDataPail);
		normalizeURLs();
		viewsPerHour();
	}

	public static void main(String[] argv) throws Exception {
		batchWorkflow();
	}
}