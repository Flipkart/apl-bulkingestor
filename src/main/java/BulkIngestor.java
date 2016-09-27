
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

/**
 * Created by anandhi on 12/12/15.
 */
public class BulkIngestor {
    private static Logger logger = Logger.getLogger(BulkIngestor.class);
    private static int BATCH_SIZE = 10;

    public void startIngestion() throws Exception {
        BulkIngestorConnector bulkIngestorConnector = new BulkIngestorConnector();
        String ids = bulkIngestorConnector.getAccrualsToBeIngested(BATCH_SIZE);
        logger.info("************* ids for invoice ***********".toUpperCase() +"  "+ ids);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(6, 6, 10, TimeUnit.MINUTES, new ArrayBlockingQueue<Runnable>(100));
        executor.prestartAllCoreThreads();
        while(ids.length() > 10){
            BulkIngestorTask bulkIngestorTask = new BulkIngestorTask(ids);
            executor.execute(bulkIngestorTask);
            ids = bulkIngestorConnector.getAccrualsToBeIngested(BATCH_SIZE);
            logger.info("************* ids for invoice ***********".toUpperCase() +"  "+ ids);
            while(executor.getQueue().size() > executor.getCorePoolSize()){
                logger.info("Queue has enough batches to process, so waiting for 10 seconds");
                Thread.sleep(10000);
            }

        }
        executor.shutdown();
        logger.info("************* Done with ingestion of all accruals ***********".toUpperCase());

        BatchIngestorFactory.INSTANCE.getBatchIngestor().close();
        bulkIngestorConnector.shutdown();

    }

    public static void main(String[] args){
    	System.setProperty("config.svc.buckets", "prod-fdpbatchingestion");
        BulkIngestor bulkIngestor = new BulkIngestor();
        try {
            bulkIngestor.startIngestion();
        } catch (Exception e) {
        	logger.error(e);
            logger.error("Error in ingestion, Message: " + e.getMessage());
        }
    }
}
