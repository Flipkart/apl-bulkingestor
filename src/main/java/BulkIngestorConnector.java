import org.apache.log4j.Logger;

import java.sql.*;

/**
 * Created by anandhi on 11/12/15.
 */
public class BulkIngestorConnector {
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost/bulk_ingestion";
    static final String USER = "root";
    static final String PASS = "KayajC9s";
    private Connection connection;
    private static Logger logger = Logger.getLogger(BulkIngestorConnector.class);

    public BulkIngestorConnector(){
        initializeConnection();
    }

    public  void initializeConnection() {
        try {
            Class.forName(JDBC_DRIVER);
            connection = DriverManager.getConnection(DB_URL, USER, PASS);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void shutdown() {
        if(connection != null){
            try {
                connection.close();
            } catch (SQLException e) {
                logger.error("Error: " + e.getMessage());
            }
        }
    }

    public String getAccrualsToBeIngested(int batchSize){
        String ids = "";
        try {
            String query = "select GROUP_CONCAT(CONCAT('''', A.id, '''')) from (select id from invoice_ingestion where status = 0 limit " + batchSize + ") A;";
            Statement statement = connection.createStatement();
            statement.execute(query);
            ResultSet rs = statement.getResultSet();
            rs.next();
            ids = rs.getString(1);
            //ids = ids + "'";
            lockIdsAsPicked(ids);
        }catch (SQLException e){
            logger.error("Error: " + e.getMessage());
        }
        return ids;
    }

    public void lockIdsAsPicked(String ids){
        try {
            String query = "update invoice_ingestion set status = 2 where id in (" + ids + ")";
            Statement statement = connection.createStatement();
            statement.executeUpdate(query);
        }catch (SQLException e){
            logger.error("Error: " + e.getMessage());
        }
    }

    public void markIdsAsIngested(String ids, String ackInfo){
        try {
            String query = "update invoice_ingestion set status = 1, ack_info = '" + ackInfo
                    + "' where id in (" + ids + ")";
            Statement statement = connection.createStatement();
            statement.executeUpdate(query);
        }catch (SQLException e){
            logger.error("Error: " + e.getMessage());
        }
    }

}

