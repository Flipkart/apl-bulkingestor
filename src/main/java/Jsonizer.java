import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;

/**
 * Created by anandhi on 10/12/15.
 */
public class Jsonizer {

    public StringWriter serialize(HashMap data){
        StringWriter stringWriter = new StringWriter();
        SimpleModule module = new SimpleModule();

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(module);
        try {
            objectMapper.writeValue(stringWriter, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return stringWriter;
    }


}
