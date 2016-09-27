import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * Created by anandhi on 10/12/15.
 */
public class AccrualConnector {

    private static Logger logger = Logger.getLogger(AccrualConnector.class);

    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_NAME = "payment_b2b_production";
    //static final String DB_NAME = "apl_invoice_register_fki_prod";
    static final String USER = "inv_reg_rw";
    static final String PASS = "AEkPbi36";
    static List<String> hosts = new LinkedList<String>(){{
       add("10.85.118.24");
        add("10.85.133.91");
    	//add("127.0.0.1");
        }
    };
    private static int accessCount = 0;


    private Connection connection;
    private Transformer transformer = new Transformer();

    public AccrualConnector(){
        initializeConnection();

    }

    public Transformer getTransformer(){
        return this.transformer;
    }

    public  void initializeConnection() {
        try {
            int no = accessCount % hosts.size();
            accessCount++;
            String dbUrl = "jdbc:mysql://" + hosts.get(no) + "/" + DB_NAME;
            Class.forName(JDBC_DRIVER);
            connection = DriverManager.getConnection(dbUrl, USER, PASS);
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

    public List<HashMap> getAccruals(String ids){
        List<HashMap> results = new LinkedList<HashMap>();
        ResultSet rs = null;
        try {
            String query = "select * from core_invoices where id in (" + ids + ")";
            logger.info(query);
            Statement statement = connection.createStatement();
            statement.execute(query);
            rs = statement.getResultSet();
            ResultSetMetaData md = rs.getMetaData();
            int columns = md.getColumnCount();
            while (rs.next()) {
                HashMap row = new HashMap();
                for (int i = 1; i <= columns; ++i) {
                    if(rs.getObject(i) != null){
                        String columnName = md.getColumnName(i);
                        if(transformer.needTransformation(columnName)){
                            Map result = transformer.transform(columnName, rs.getObject(i));
                            row.put(columnName, result.get(columnName));
                        }else{
                            row.put(columnName, rs.getObject(i));
                        }
                    }
                }
                row.remove("partition_key");
                results.add(row);
            }
        }catch (SQLException se){
            logger.error("Error: " + se.getMessage());
        }

        finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    logger.error("Error: " + e.getMessage());
                }
            }
        }

        return results;
    }


    public HashMap<String, List> getAccrualItems(String ids){
        HashMap<String, List> results = new HashMap<String, List>();
        ResultSet rs = null;
        try {
            String query = "select * from core_invoice_items where core_invoice_id in (" + ids +
                    ") and linked_core_invoice_item_id is null";
            logger.info(query);
            Statement statement = connection.createStatement();
            statement.execute(query);
            rs = statement.getResultSet();
            ResultSetMetaData md = rs.getMetaData();
            int columns = md.getColumnCount();
            while (rs.next()) {
                HashMap row = new HashMap();
                List<HashMap> valueList = null;
                String parentId = rs.getString("core_invoice_id");
                for (int i = 1; i <= columns; ++i) {
                	
                    if(rs.getObject(i) != null && !transformer.needsToIgnore(md.getColumnName(i))) {
                        String columnName = md.getColumnName(i);
                        if (transformer.needTransformation(columnName)) {
                            Map result = transformer.transform(columnName, rs.getObject(i));
                            row.put(columnName, result.get(columnName));
                        } else {
                            row.put(columnName, rs.getObject(i));
                        }
                    }
                	
                }
                if(results.containsKey(parentId)){
                    valueList = results.get(parentId);
                }else{
                    valueList = new LinkedList<HashMap>();
                }
                row.remove("invoice_item_attributes");
                row.remove("type");
                valueList.add(row);

                results.put(parentId, valueList);
            }
        }catch (SQLException se){
            logger.error("Error: " + se.getMessage());
        }

        finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    logger.error("Error: " + e.getMessage());
                }
            }
        }

        return results;
    }

    public HashMap<String, List> getAccrualSubItems(String ids){
        HashMap<String, List> results = new HashMap<String, List>();
        ResultSet rs = null;
        try {
            String query = "select * from core_invoice_items where core_invoice_id in (" + ids +
                    ") and linked_core_invoice_item_id is not null";
            logger.info(query);
            Statement statement = connection.createStatement();
            statement.execute(query);
            rs = statement.getResultSet();
            ResultSetMetaData md = rs.getMetaData();
            int columns = md.getColumnCount();
            while (rs.next()) {
                HashMap row = new HashMap();
                List<HashMap> valueList = null;
                String parentId = rs.getString("linked_core_invoice_item_id");
                for (int i = 1; i <= columns; ++i) {
                    if(rs.getObject(i) != null && !transformer.needsToIgnore(md.getColumnName(i))) {
                        String columnName = md.getColumnName(i);
                        if (transformer.needTransformation(columnName)) {
                            Map result = transformer.transform(columnName, rs.getObject(i));
                            row.put(columnName, result.get(columnName));
                        } else {
                            row.put(columnName, rs.getObject(i));
                        }
                    }
                }
                if(results.containsKey(parentId)){
                    valueList = results.get(parentId);
                }else{
                    valueList = new LinkedList<HashMap>();
                }
                row.remove("type");
                valueList.add(row);

                results.put(parentId, valueList);
            }
        }catch (SQLException se){
            logger.error("Error: " + se.getMessage());
        }

        finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    logger.error("Error: " + e.getMessage());
                }
            }
        }

        return results;
    }

    public static void main(String[] args) {

        while(accessCount < 10) {
            int no = accessCount % hosts.size();
            logger.info(no);
            accessCount++;
        }
    }

}
