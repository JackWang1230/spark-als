import com.uniondrug.utils.DateUtil;

import java.text.ParseException;

/**
 * @author RWang
 * @Date 2021/1/13
 */

public class DateTest {

    public void  testData() throws ParseException {

        String a ="2021-01-13 13:43:42";
        String b ="2021-01-13 13:53:42";

        long aa = Long.parseLong(DateUtil.dateToStamp(a));
        long bb = Long.parseLong(DateUtil.dateToStamp(b));
        System.out.println(aa);
        System.out.println(bb);
        for (int i = 0; i < 3; i++) {
            if (aa>bb){
                System.out.println("1");
            }

        }


    }
    public static void main(String[] args) throws ParseException {



    }
}
