import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by anandhi on 11/12/15.
 */
public class DataMerger {

    public List<HashMap> mergeAll(List<HashMap> accruals, HashMap<String, List> items,HashMap<String, List> subItems){
        Iterator<HashMap> accrualIterator = accruals.iterator();
        List<HashMap> bigFootFormatData = new LinkedList<HashMap>();
        while(accrualIterator.hasNext()){
            HashMap accrual = accrualIterator.next();
            HashMap bigFootEntityData = new HashMap();
            String accrualId = (String) accrual.get("id");
            bigFootEntityData.put("entityId", accrualId);
            bigFootEntityData.put("schemaVersion", "1.0");
            bigFootEntityData.put("updatedAt", accrual.get("updated_at").
                    toString().replace(" ", "T").concat(".").
                    concat(Util.getTimeBasedIncrementalNumber(9).toString()).
                    concat("+05:30"));
            List<HashMap> linkedItems = items.get(accrualId);
            Iterator<HashMap> itemsIterator = linkedItems.iterator();
            while(itemsIterator.hasNext()){
                HashMap item = itemsIterator.next();
                item.put("sub_items", subItems.get(item.get("id")));
            }
            accrual.put("invoice_items", linkedItems);
            bigFootEntityData.put("data", accrual);
            bigFootFormatData.add(bigFootEntityData);
        }
        return bigFootFormatData;
    }
}
