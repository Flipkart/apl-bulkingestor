import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by anandhi on 12/12/15.
 */
public class Util {
    private static final String DATE_TIME_FORMAT_WITH_SPACE = "yyyy-MM-dd HH:mm:ss";

    public static String formatDatetime(String datetime, String inputDateFormat) throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat(inputDateFormat);
        Date date = dateFormat.parse(datetime);

        SimpleDateFormat dt1 = new SimpleDateFormat(DATE_TIME_FORMAT_WITH_SPACE);
        return dt1.format(date);
    }

    public static String convertTime(long time){
        Date date = new Date(time);
        Format format = new SimpleDateFormat(DATE_TIME_FORMAT_WITH_SPACE);
        return format.format(date);
    }

    public static Integer getTimeBasedIncrementalNumber(int digits) {
        Integer maxValue = 1;
        while(digits > 0) {
            maxValue = maxValue * 10;
            digits--;
        }
        return (int) (System.currentTimeMillis() % maxValue);
    }
}
