import com.flipkart.dart.Batch;
import com.flipkart.dart.BatchIngestor;
import com.flipkart.dart.IngestionSuspendedException;
import com.flipkart.dart.UploadResponse;
import com.flipkart.dart.data.validation.ValidationException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;


import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;


/**
 * Created by anandhi on 13/12/15.
 */
public class BulkIngestorTask implements Runnable {
    private String ids;
    private static Logger logger = Logger.getLogger(BulkIngestorTask.class);
    private static String entityUrl = "fkint/apl/finance_reporting/invoice";


    public BulkIngestorTask(String ids){
        this.ids = ids;
    }
    
   
    public void run() {

        try {
            ingestForIds();
        } catch (Exception e) {
        	logger.error(ExceptionUtils.getStackTrace(e));
            logger.error("Unable to ingest for the ids, Error: " + e.getMessage());
        }
    }

    private void ingestForIds() throws Exception {
    	logger.info("Getting batch ingestor object");
    	BatchIngestor batchIngestor = BatchIngestorFactory.INSTANCE.getBatchIngestor();
        logger.info("Got batch ingestor object");
        DataMerger merger = new DataMerger();
        Jsonizer jsonizer = new Jsonizer();
        BulkIngestorConnector bulkIngestorConnector = new BulkIngestorConnector();
        AccrualConnector connector = new AccrualConnector();
        connector.getTransformer().populateTransformationColumnsForAccruals();
        Long timeBegin = System.currentTimeMillis();
        List<HashMap> accrualsToIngest = merger.mergeAll(connector.getAccruals(ids),
                connector.getAccrualItems(ids),
                connector.getAccrualSubItems(ids));
        Gson gson = new GsonBuilder().serializeNulls().create();
        logger.info("******* printing json *********" );
        logger.info(gson.toJson(accrualsToIngest));
        
        Iterator<HashMap> accrualIterator = accrualsToIngest.iterator();
        String batchId = "INVOICE_".concat(String.valueOf(System.currentTimeMillis())).
                concat("_").concat(Util.getTimeBasedIncrementalNumber(5).toString());
        logger.info("Time taken to extract and merge is " + (System.currentTimeMillis() - timeBegin) + "ms");
        Long prepareBatchBegin = System.currentTimeMillis();
        logger.info("Getting batch ingestor new batch object" + batchIngestor);
        Batch batch = batchIngestor.newBatch(batchId, entityUrl);
        logger.info("Got batch ingestor new batch object");
        while(accrualIterator.hasNext()){
            HashMap accrual = accrualIterator.next();
            StringWriter accrualJson = jsonizer.serialize(accrual);
            batch.add(accrualJson.toString());
        }
        logger.info("Time taken to prepare batch is " + (System.currentTimeMillis() - prepareBatchBegin) + "ms");
        Long batchCommitBegin = System.currentTimeMillis();
        UploadResponse uploadResponse = batch.commit();
        logger.info("Time taken to commit batch is " + (System.currentTimeMillis() - batchCommitBegin) + "ms");
        bulkIngestorConnector.markIdsAsIngested(ids, uploadResponse.getLocation().get(0));
        logger.info("Time taken to ingest batch -  " + batchId + " of size "  + accrualsToIngest.size() + " is "
                + (System.currentTimeMillis() - timeBegin) + "ms");

        connector.shutdown();
        bulkIngestorConnector.shutdown();
    }
}
