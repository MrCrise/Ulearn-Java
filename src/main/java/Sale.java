import java.util.Date;


public class Sale {
    String region;
    String country;
    String itemType;
    String salesChannel;
    String orderPriority;
    Date orderDate;
    int unitsSold;
    double totalProfit;

    public Sale(String region, String country, String itemType, String salesChannel,
                   String orderPriority, Date orderDate, int unitsSold, double totalProfit) {
        this.region = region;
        this.country = country;
        this.itemType = itemType;
        this.salesChannel = salesChannel;
        this.orderPriority = orderPriority;
        this.orderDate = orderDate;
        this.unitsSold = unitsSold;
        this.totalProfit = totalProfit;
    }
}