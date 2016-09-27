import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.*;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

/**
 * Created by anandhi on 12/12/15.
 */
public class Transformer {
    List<String> dateColumns = new LinkedList<String>();
    List<String> attributeColumns = new LinkedList<String>();
    List<String> snakeCaseColumns = new LinkedList<String>();
    List<String> floatColumns = new LinkedList<String>();
    List<String> integerColumns = new LinkedList<String>();
   	List<String> ignoreColumns = new LinkedList<String>();
   	List<String> stringColumns = new LinkedList<String>();
   	List<String> requiredColumns = new LinkedList<String>();
	
   	final Logger logger = Logger.getLogger(Transformer.class);
   	
    public Map transform(String columnName, Object columnValue){
    	
        HashMap map = new HashMap();
        if (columnValue !=null) {
        if(dateColumns.contains(columnName)){
            map.put(columnName, columnValue.toString().replace(".0", ""));
        }else if(attributeColumns.contains(columnName)){
            ObjectMapper mapper = new ObjectMapper();
            Map attributeMap = new HashMap();
            Map<String, Object> convertedMap;
            try {
                convertedMap = mapper.readValue(columnValue.toString(), new TypeReference<Map<String, Object>>(){});
                logger.info("**** COLUMN "+columnName+" ***** "+columnValue);
                Iterator<String> keyIterator = convertedMap.keySet().iterator();
                while(keyIterator.hasNext()){
                    String colName = keyIterator.next();
                    if(colName.contains("reporting_ref")){
                        attributeMap.put(colName, convertedMap.get(colName));
                    }
                }
                //attributeMap.put("raw_data", columnValue.toString());
            } catch (IOException e) {
            	logger.error(ExceptionUtils.getStackTrace(e));
            }
            map.put("invoice_attributes", attributeMap);
        }else if(snakeCaseColumns.contains(columnName)){
            map.put(columnName, columnValue.toString().replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase());
        }else if(floatColumns.contains(columnName)){
        	if (columnValue.toString().equals("0")) {
        		
        		map.put(columnName, Float.valueOf("0.0"));
        	}
        	else
            map.put(columnName, Float.valueOf(columnValue.toString()));
        }else if(integerColumns.contains(columnName)) {
        	map.put(columnName, Integer.valueOf(columnValue.toString()));
        }else if(stringColumns.contains(columnName)) {
        	map.put(columnName, columnValue.toString());
        }
        
    	}
        return map;
    }

    public boolean needTransformation(String columnName){
        if(dateColumns.contains(columnName) ||
                attributeColumns.contains(columnName) ||
                snakeCaseColumns.contains(columnName) ||
                floatColumns.contains(columnName) ||
        		integerColumns.contains(columnName) ||
        		stringColumns.contains(columnName)){
            return true;
        }
        return false;
    }

    public boolean needsToIgnore(String columnName){
        if(ignoreColumns.contains(columnName)){
        	logger.info(" **** Inside needToIgnore for *********** "+columnName);
            return true;
        }
        return false;
    }
    
    public boolean addRequired(String columnName) {
    	if(requiredColumns.contains(columnName)) {
    		return true;
    	}
    	return false;
    }

    public void addDateColumn(String columnName){
        dateColumns.add(columnName);
    }
    
    public void addrequiredColumn(String columnName){
        requiredColumns.add(columnName);
    }

    public void addStringColumn(String columnName){
        stringColumns.add(columnName);
    }

    
    public void addAttributeColumn(String columnName){
        attributeColumns.add(columnName);
    }

    public void addSnakeCaseColumns(String columnName){
        snakeCaseColumns.add(columnName);
    }

    public void addFloatColumns(String columnName){
        floatColumns.add(columnName);
    }

	public void addIgnoreColumns(String columnName){
		ignoreColumns.add(columnName);
    }
    
    public List<String> getIntegerColumns() {
		return integerColumns;
	}

	public void addIntegerColumns(String columnName) {
		integerColumns.add(columnName);
	}


    public void populateTransformationColumnsForAccruals(){
        //Date Columns
        addDateColumn("created_at");
        addDateColumn("updated_at");
        addDateColumn("invoice_date");
        addDateColumn("due_date");
        addDateColumn("gl_date");
        addDateColumn("deleted_at");
        addDateColumn("gl_reverse_date");
        addDateColumn("invoice_ref_date_1");
        addDateColumn("invoice_ref_date_2");
        addDateColumn("invoice_ref_date_3");
        addDateColumn("invoice_ref_date_4");

        //Attribute Columns
        addAttributeColumn("invoice_attributes");
        addAttributeColumn("invoice_item_attributes");

        //Snake Case Columns
        addSnakeCaseColumns("type");
        addSnakeCaseColumns("invoice_type");

        //Float Columns
        addFloatColumns("amount");
        addFloatColumns("total_amount");
        addFloatColumns("open_amount");
        addFloatColumns("applied_amount");
        addFloatColumns("pending_applied_amount");
        addFloatColumns("unit_price");
        addFloatColumns("tax_rate");
        
        
        //Integer Columns
        addIntegerColumns("quantity");
        addIntegerColumns("reco_status");
        //Ignore Columns
        
        
        addIgnoreColumns("partition_key");
        addIgnoreColumns("bu_id");
        addIgnoreColumns("created_at");
        addIgnoreColumns("updated_at");
        
        addrequiredColumn("linked_core_invoice_item_id");
        
        
        //addStringColumn("linked_core_invoice_item_id");
       
    }
}
