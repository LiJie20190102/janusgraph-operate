import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * Description
 *
 * @author lijie0203 2024/3/5 14:01
 */
public class Test1 {
    public static void main(String[] args) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
        Date date = new Date();
        String formattedDate = sdf.format(date);
        System.out.println(formattedDate);

        String aa = "sss";
        System.out.println(Arrays.toString(aa.getBytes()));
    }
}
