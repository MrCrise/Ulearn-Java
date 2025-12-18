import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class DatabaseManager {
    private String url;

    public DatabaseManager(String url) {
        this.url = url;
    }

    public void initDatabase() throws SQLException {
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {

            // Удаление старых таблиц, чтобы перезаписать данные.
            stmt.execute("DROP TABLE IF EXISTS sales");
            stmt.execute("DROP TABLE IF EXISTS countries");
            stmt.execute("DROP TABLE IF EXISTS regions");
            stmt.execute("DROP TABLE IF EXISTS item_types");
            stmt.execute("DROP TABLE IF EXISTS sales_channels");
            stmt.execute("DROP TABLE IF EXISTS priorities");

            // Создание таблиц (3-я нормальная форма).
            stmt.execute("CREATE TABLE IF NOT EXISTS regions (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "name TEXT UNIQUE NOT NULL)");

            stmt.execute("CREATE TABLE IF NOT EXISTS countries (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "name TEXT UNIQUE NOT NULL, " +
                    "region_id INTEGER, " +
                    "FOREIGN KEY(region_id) REFERENCES regions(id))");

            stmt.execute("CREATE TABLE IF NOT EXISTS item_types (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "name TEXT UNIQUE NOT NULL)");

            stmt.execute("CREATE TABLE IF NOT EXISTS sales_channels (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "name TEXT UNIQUE NOT NULL)");

            stmt.execute("CREATE TABLE IF NOT EXISTS priorities (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "code TEXT UNIQUE NOT NULL)");

            stmt.execute("CREATE TABLE IF NOT EXISTS sales (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "country_id INTEGER, " +
                    "item_type_id INTEGER, " +
                    "sales_channel_id INTEGER, " +
                    "priority_id INTEGER, " +
                    "order_date INTEGER, " + // храним как timestamp
                    "units_sold INTEGER, " +
                    "total_profit REAL, " +
                    "FOREIGN KEY(country_id) REFERENCES countries(id), " +
                    "FOREIGN KEY(item_type_id) REFERENCES item_types(id), " +
                    "FOREIGN KEY(sales_channel_id) REFERENCES sales_channels(id), " +
                    "FOREIGN KEY(priority_id) REFERENCES priorities(id))");
        }
    }

    public void importData(List<Sale> sales) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            Map<String, Integer> regionCache = new HashMap<>();
            Map<String, Integer> countryCache = new HashMap<>();
            Map<String, Integer> itemTypeCache = new HashMap<>();
            Map<String, Integer> channelCache = new HashMap<>();
            Map<String, Integer> priorityCache = new HashMap<>();

            String insertSaleSQL = "INSERT INTO sales (country_id, item_type_id, sales_channel_id, priority_id, order_date, units_sold, total_profit) VALUES (?, ?, ?, ?, ?, ?, ?)";

            try (PreparedStatement pstSale = conn.prepareStatement(insertSaleSQL)) {
                for (Sale s : sales) {
                    int regionId = getOrInsertId(conn, "regions", "name", s.region, regionCache);
                    int countryId = getOrInsertCountry(conn, s.country, regionId, countryCache);
                    int itemId = getOrInsertId(conn, "item_types", "name", s.itemType, itemTypeCache);
                    int channelId = getOrInsertId(conn, "sales_channels", "name", s.salesChannel, channelCache);
                    int priorityId = getOrInsertId(conn, "priorities", "code", s.orderPriority, priorityCache);

                    pstSale.setInt(1, countryId);
                    pstSale.setInt(2, itemId);
                    pstSale.setInt(3, channelId);
                    pstSale.setInt(4, priorityId);
                    pstSale.setLong(5, s.orderDate.getTime());
                    pstSale.setInt(6, s.unitsSold);
                    pstSale.setDouble(7, s.totalProfit);
                    pstSale.addBatch();
                }
                pstSale.executeBatch();
            }
            conn.commit();
        }
    }

    private int getOrInsertId(Connection conn, String table, String col, String val, Map<String, Integer> cache) throws SQLException {
        if (cache.containsKey(val)) return cache.get(val);

        int id = -1;
        try (PreparedStatement pst = conn.prepareStatement("SELECT id FROM " + table + " WHERE " + col + " = ?")) {
            pst.setString(1, val);
            ResultSet rs = pst.executeQuery();
            if (rs.next()) id = rs.getInt("id");
        }

        if (id == -1) {
            try (PreparedStatement pst = conn.prepareStatement("INSERT INTO " + table + " (" + col + ") VALUES (?)", Statement.RETURN_GENERATED_KEYS)) {
                pst.setString(1, val);
                pst.executeUpdate();
                ResultSet rs = pst.getGeneratedKeys();
                if (rs.next()) id = rs.getInt(1);
            }
        }
        cache.put(val, id);
        return id;
    }

    private int getOrInsertCountry(Connection conn, String name, int regionId, Map<String, Integer> cache) throws SQLException {
        if (cache.containsKey(name)) return cache.get(name);

        int id = -1;
        try (PreparedStatement pst = conn.prepareStatement("SELECT id FROM countries WHERE name = ?")) {
            pst.setString(1, name);
            ResultSet rs = pst.executeQuery();
            if (rs.next()) id = rs.getInt("id");
        }

        if (id == -1) {
            try (PreparedStatement pst = conn.prepareStatement("INSERT INTO countries (name, region_id) VALUES (?, ?)", Statement.RETURN_GENERATED_KEYS)) {
                pst.setString(1, name);
                pst.setInt(2, regionId);
                pst.executeUpdate();
                ResultSet rs = pst.getGeneratedKeys();
                if (rs.next()) id = rs.getInt(1);
            }
        }
        cache.put(name, id);
        return id;
    }
}