package windowagg;
import java.io.Serializable;
//public class UserBehavior implements Serializable {


public class UserBehavior{
	
    private long user_id;
    private long item_id;
    private String category_id;
    private String behavior;
    private String ts;
    private long total_agg;
    // Default constructor
    public UserBehavior() {
    }

    // Parameterized constructor
    public UserBehavior(long user_id, long item_id, String category_id, String behavior, String ts) {
        this.user_id = user_id;
        this.item_id = item_id;
        this.category_id = category_id;
        this.behavior = behavior;
        this.ts = ts;
        this.total_agg = 1;
    }

    public long get_agg() {
        return total_agg;
    }

    public void set_agg(long agg_value) {
        this.total_agg  = agg_value;
    }

    // Getters and setters
    public long getUser_id() {
        return user_id;
    }

    public void setUser_id(long user_id) {
        this.user_id = user_id;
    }

    public long getItem_id() {
        return item_id;
    }

    public void setItem_id(long item_id) {
        this.item_id = item_id;
    }

    public String getCategory_id() {
        return category_id;
    }

    public void setCategory_id(String category_id) {
        this.category_id = category_id;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public String getTs() {
        return ts;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "user_id=" + user_id +
                ", item_id=" + item_id +
                ", category_id=" + category_id +
                ", behavior='" + behavior + '\'' +
                ", ts='" + ts + '\'' +
                '}';
    }
}