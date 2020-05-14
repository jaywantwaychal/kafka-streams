
package serde;


import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import com.example.demo.types.Notification;
import com.example.demo.types.PosInvoice;
import com.example.demo.types.Report;

/**
 * Factory class for Serdes
 */

public class AppSerdes extends Serdes {


    static final class PosInvoiceSerde extends Serdes.WrapperSerde<PosInvoice> {
        PosInvoiceSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<PosInvoice> PosInvoice() {
        PosInvoiceSerde serde = new PosInvoiceSerde();

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, PosInvoice.class);
        serde.configure(serdeConfigs, false);

        return serde;
    }

    static final class NotificationSerde extends Serdes.WrapperSerde<Notification> {
        NotificationSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<Notification> Notification() {
        NotificationSerde serde = new NotificationSerde();

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, Notification.class);
        serde.configure(serdeConfigs, false);

        return serde;
    }

    static final class ReportSerde extends Serdes.WrapperSerde<Report> {
    	ReportSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }
    public static Serde<Report> Report() {
    	ReportSerde serde = new ReportSerde();

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, Report.class);
        serde.configure(serdeConfigs, false);

        return serde;
    }
}
