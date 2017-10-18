import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by 11325 on 2017/9/26.
 */
public class TimeUtilTest {
    public static void main(String[] args){
        String datdString="13/Sep/2017:07:04:59 +0800";
        //System.out.print(d.getYear() + '-' + (d.getMonth() + 1) + '-' + d.getDate() + ' ' + d.getHours() + ':' + d.getMinutes() + ':' + d.getSeconds());
        //datdString = datdString.replaceAll("\\(.*\\)", "");
        SimpleDateFormat format = new SimpleDateFormat("d/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
        Date dateTrans = null;
        try {
            dateTrans = format.parse(datdString);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss S SS").format(dateTrans));

        String rowkeyTime = "2017-09-13+99.64.157.233".replace("+",",").split(",")[0];
        System.out.println(rowkeyTime);
    }
}
