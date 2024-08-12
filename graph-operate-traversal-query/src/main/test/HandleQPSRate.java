import com.qsdi.bigdata.janusgaph.ops.util.LineIterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Description
 *
 * @author lijie0203 2024/3/5 17:35
 */
public class HandleQPSRate {
    public static void main(String[] args) {
        File dir = new File("D:\\tmp\\1031\\csv_test\\csv");
        Workbook workbook = new XSSFWorkbook();

        int count = 0;
        String val = "0";
        int col = 0;
        for (File file : dir.listFiles()) {
//t,count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit,duration_unit
            try (LineIterator<List<String>> lineIterator =
                         new LineIterator<>(file.getAbsolutePath(), 1)) {
                Sheet sheet = workbook.createSheet(file.getName());
                boolean init = false;
                int row = 1;
                while (lineIterator.hasNext()) {
                    List<String> next = lineIterator.next();
                    String line = next.get(0);
                    if (line.contains("duration_unit")) {
                        continue;
                    }

                    if (!init) {
                        Row dataRow = sheet.createRow(0);
                        createCell(dataRow, col, file.getName());
                        init = true;
                    }

                    Row dataRow = sheet.createRow(row);
                    String[] split = line.split(",");
                    String s1 = split[1];
                    if (StringUtils.equalsIgnoreCase(s1, val)) {
                        count++ ;
                        if (count>=10) {
                            break;
                        }
                    }else {
                        val=s1;
                        count = 0;
                    }


                    String s = split[12];
                    createCell(dataRow, col, Double.parseDouble(s));

                    row++;
                }

                System.out.println("====" + file.getName());
            } catch (Exception e) {
                throw new RuntimeException("===error:", e);
            }
            col++;
        }

//        new File("D:\\tmp\\1031\\csv_test\\output.xlsx")
        try (FileOutputStream outputStream = new FileOutputStream("D:\\tmp\\1031\\csv_test\\output.xlsx")) {
            workbook.write(outputStream);
            System.out.println("Excel generated successfully.");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private static void createCell(Row row, int columnNumber, double cellValue) {
        Cell cell = row.createCell(columnNumber);
        cell.setCellValue(cellValue);
    }

    private static void createCell(Row row, int columnNumber, String cellValue) {
        Cell cell = row.createCell(columnNumber);
        cell.setCellValue(cellValue);
    }

}
