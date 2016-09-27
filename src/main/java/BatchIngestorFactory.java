import com.flipkart.dart.BatchIngestor;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

/**
 * Created by anandhi on 12/12/15.
 */
public enum BatchIngestorFactory {
	INSTANCE;
	final Logger logger = Logger.getLogger(BatchIngestorFactory.class);
	private BatchIngestor batchIngestor;
	private BatchIngestorFactory(){
		try {		
			batchIngestor = new BatchIngestor();			
		} catch (Exception ex) {
			logger.error(ExceptionUtils.getStackTrace(ex));
			logger.error("Unable to instantiate the HDFS Batch Ingestor, Errorr: " + ex.getMessage());
			
		}		
	}
	
	public BatchIngestor getBatchIngestor(){
		return batchIngestor;
	}
	
}
