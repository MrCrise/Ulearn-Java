import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {

    private static final String CSV_FILE = "Продажа продуктов в мире.csv";
    private static final String DB_URL = "jdbc:sqlite:sales_variant6.db";

    public static void main(String[] args) {
        try {
            System.out.println("Чтение и парсинг CSV файла\n");
            List<Sale> rawSales = CsvParser.parse(CSV_FILE);
            System.out.println("Загружено записей: " + rawSales.size());

            System.out.println("Инициализация БД и импорт данных\n");
            DatabaseManager dbManager = new DatabaseManager(DB_URL);
            dbManager.initDatabase();
            dbManager.importData(rawSales);

            System.out.println("Задания. Вариант 6");

            // Задача 1: График по регионам.
            task1(dbManager);

            // Задача 2: Страна с самым высоким общим доходом среди регионов Европы и Азии.
            task2(dbManager);

            // Задача 3: Страна с доходом 420-440 тысяч (Ближний Восток и Северная Африка и Субсахарская Африка).
            task3(dbManager);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Задание 1.
    private static void task1(DatabaseManager db) throws SQLException {
        System.out.println("\n>>> Задача 1: Общее количество проданных товаров по регионам");
        String sql = "SELECT r.name, SUM(s.units_sold) as total_units " +
                "FROM sales s " +
                "JOIN countries c ON s.country_id = c.id " +
                "JOIN regions r ON c.region_id = r.id " +
                "GROUP BY r.name " +
                "ORDER BY total_units";

        Map<String, Integer> chartData = new HashMap<>();

        try (Connection conn = DriverManager.getConnection(DB_URL);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            System.out.printf("%-40s | %-15s%n", "Регион", "Продано (шт.)");
            System.out.println("----------------------------------------------------------");
            while (rs.next()) {
                String region = rs.getString("name");
                int units = rs.getInt("total_units");
                chartData.put(region, units);
                System.out.printf("%-40s | %-15d%n", region, units);
            }
        }

        System.out.println("[INFO] Данные для построения графика получены");
        try {
            task1Chart.createChart(chartData);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Задание 2.
    private static void task2(DatabaseManager db) throws SQLException {
        System.out.println("\n>>> Задача 2: Страна с самым высоким общим доходом среди регионов Европы и Азии");
        String sql = "SELECT c.name, SUM(s.total_profit) as total_profit " +
                "FROM sales s " +
                "JOIN countries c ON s.country_id = c.id " +
                "JOIN regions r ON c.region_id = r.id " +
                "WHERE r.name IN ('Europe', 'Asia') " +
                "GROUP BY c.name " +
                "ORDER BY total_profit DESC " +
                "LIMIT 1";

        try (Connection conn = DriverManager.getConnection(DB_URL);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                System.out.println("Страна: " + rs.getString("name"));
                System.out.printf("Общий доход: %.2f%n", rs.getDouble("total_profit"));
            } else {
                System.out.println("Данные не найдены.");
            }
        }
    }

    // Задание 3.
    // Возможно в данной задаче некорректная формулировка, т.к. если считать именно суммарный доход для стран,
    // а не просто брать значения из столбца Total Profit, то подходящих стран нет.
    private static void task3(DatabaseManager db) throws SQLException {
        System.out.println("\n>>> Задача 3: Страна с доходом 420-440 тысяч (Ближний Восток и Северная Африка и Субсахарская Африка)");
        String sql = "SELECT c.name, SUM(s.total_profit) as total_profit " +
                "FROM sales s " +
                "JOIN countries c ON s.country_id = c.id " +
                "JOIN regions r ON c.region_id = r.id " +
                "WHERE r.name IN ('Middle East and North Africa', 'Sub-Saharan Africa') " +
                "GROUP BY c.name " +
                "HAVING total_profit BETWEEN 420000 AND 440000 " +
                "ORDER BY total_profit DESC " +
                "LIMIT 1";

        try (Connection conn = DriverManager.getConnection(DB_URL);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                System.out.println("Страна: " + rs.getString("name"));
                System.out.printf("Общий доход: %.2f%n", rs.getDouble("total_profit"));
            } else {
                System.out.println("Подходящая страна не найдена.");
            }
        }
    }
}