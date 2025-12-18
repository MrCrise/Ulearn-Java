import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class CsvParser {
    private static final SimpleDateFormat sdfSlash = new SimpleDateFormat("M/d/yyyy");
    private static final SimpleDateFormat sdfDot = new SimpleDateFormat("dd.MM.yyyy");

    public static List<Sale> parse(String filePath) throws IOException {
        List<Sale> sales = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty() || line.startsWith("Region,Country")) continue;

                String[] parts = line.split(",");
                if (parts.length < 8) continue;

                try {
                    String region = parts[0].trim();
                    String country = parts[1].trim();
                    String itemType = parts[2].trim();
                    String channel = parts[3].trim();
                    String priority = parts[4].trim();
                    String dateStr = parts[5].trim();
                    int units = Integer.parseInt(parts[6].trim());
                    double profit = Double.parseDouble(parts[7].trim());

                    Date date = parseDate(dateStr);
                    sales.add(new Sale(region, country, itemType, channel, priority, date, units, profit));

                } catch (Exception e) {
                    System.err.println("Ошибка парсинга строки: " + line + " -> " + e.getMessage());
                }
            }
        }
        return sales;
    }

    private static Date parseDate(String dateStr) throws ParseException {
        if (dateStr.contains("/")) {
            return sdfSlash.parse(dateStr);
        } else {
            return sdfDot.parse(dateStr);
        }
    }
}
