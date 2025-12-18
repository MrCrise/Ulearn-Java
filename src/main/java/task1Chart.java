import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.data.category.DefaultCategoryDataset;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class task1Chart {
    public static void createChart(Map<String, Integer> data) throws IOException {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        for (Map.Entry<String, Integer> entry : data.entrySet()) {
            dataset.addValue(entry.getValue(), "Units Sold", entry.getKey());
        }

        JFreeChart barChart = ChartFactory.createBarChart(
                "Продажи по регионам",
                "Регион",
                "Количество проданных товаров",
                dataset
        );

        int width = 1200;
        int height = 600;
        File BarChart = new File("SalesByRegion.jpeg");
        ChartUtilities.saveChartAsJPEG(BarChart, barChart, width, height);
        System.out.println("[INFO] График сохранен в файл: SalesByRegion.jpeg");
    }
}
