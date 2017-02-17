import java.io.File;

import org.apache.edgent.function.Consumer;
import org.apache.edgent.providers.direct.DirectProvider;
import org.apache.edgent.topology.TStream;
import org.apache.edgent.topology.TWindow;
import org.apache.edgent.topology.Topology;
import org.apache.edgent.connectors.iotp.*;
import org.apache.edgent.analytics.math3.json.JsonAnalytics;
import org.apache.edgent.connectors.iot.*;
import org.apache.edgent.analytics.math3.stat.Statistic;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.pi4j.io.gpio.*;
import com.pi4j.wiringpi.GpioUtil;

public class RangeSensor implements Runnable {

    private static final Double SPEED_OF_SOUND = 343.0;
    private GpioController gpio = GpioFactory.getInstance();
    private Pin echoRPin = RaspiPin.GPIO_27;
    private Pin trigRPin = RaspiPin.GPIO_26;
    GpioPinDigitalInput echoPin = null;
    GpioPinDigitalOutput trigPin = null; 

    private Consumer<Double> eventSubmitter;
    private Thread t;
    static AtomicReference<Double> thresholdRef = new AtomicReference<>(0.40);
    
    public static void main(String[] args) throws Exception {

        String deviceCfg = args[0];
        DirectProvider dp = new DirectProvider();
        Topology topology = dp.newTopology();

        IotDevice device = new IotpDevice(topology, new File(deviceCfg)); 

        RangeSensor sensor = new RangeSensor();

        //poll Range sensor every second for distance reading
        TStream<Double> distanceReadings = device.topology().events(eventSubmitter -> sensor.register(eventSubmitter));

        //filter out bad readings that are out of the sensor's 1000cm range
        distanceReadings = distanceReadings.filter(j -> j < 1.00);

        TStream<JsonObject> sensorJSON = distanceReadings.map(v -> {
            JsonObject j = new JsonObject();
            j.addProperty("name", "rangeSensor");
            j.addProperty("reading", v);
            return j;
        });

        // Create a window on the stream of the last 10 seconds
        TWindow<JsonObject,JsonElement> sensorWindow = sensorJSON.last(10, TimeUnit.SECONDS, j -> j.get("name"));

        // Aggregate the windows calculating the min, max, mean and standard deviation
        // across each window independently.
        sensorJSON = JsonAnalytics.aggregate(sensorWindow, "name", "reading", Statistic.MIN, Statistic.MAX, Statistic.MEAN, Statistic.STDDEV);

        // Filter so that only when the mean sensor reading is that an object is closer than threshold send data. 
        sensorJSON = sensorJSON.filter(j -> (j.get("reading") != null &&
                                             j.get("reading").getAsJsonObject().get("MEAN") != null &&
                                             Math.abs(j.get("reading").getAsJsonObject().get("MEAN").getAsDouble()) < thresholdRef.get()));

        // Send the device streams as IoTF device events
        // with event identifier "sensors".
        device.events(sensorJSON, "sensors", QoS.FIRE_AND_FORGET);
        
        device.commands("setMode").sink(json -> sensor.processCommand(json.get(IotDevice.CMD_PAYLOAD).getAsJsonObject()));
        
        distanceReadings.print();
        
        dp.submit(topology);
    }

    public RangeSensor() {
        GpioUtil.enableNonPrivilegedAccess();
        echoPin = gpio.provisionDigitalInputPin(echoRPin);
        trigPin = gpio.provisionDigitalOutputPin(trigRPin);
    }

    public void register(Consumer<Double> eventSubmitter) {
        this.eventSubmitter = eventSubmitter;
        
        t = new Thread (this, "reader");
        t.setPriority(Thread.NORM_PRIORITY + 1);
        t.start();
    }
    
    @Override
    public void run() {
        System.out.println("Start reader thread!");
        
        while (true) {
            eventSubmitter.accept(getDistance());
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }

        }
    }

    public void processCommand(JsonObject json) {
        
        System.out.println("cmd:-------");
        System.out.println(json.toString());
        
        if (json.get("threshold") != null) {
        double threshold = json.get("threshold").getAsDouble();
          System.out.println("new threshold: " + String.valueOf(threshold));
          thresholdRef.set(threshold);
        }
    }
    
    private Double getDistance() {

        trigPin.low();
        try {
            TimeUnit.MICROSECONDS.sleep(5);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        trigPin.high();
        try {
            TimeUnit.MICROSECONDS.sleep(10);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        trigPin.low();
        
        int MAX_CYCLES = 10000;
        int cycles = 0;
        while (this.echoPin.isLow() && cycles < MAX_CYCLES) {
            cycles++;
        }
        
        long start = System.nanoTime();
        cycles = 0;
        while (this.echoPin.isHigh() && cycles < MAX_CYCLES) {
            cycles++;
        }
        long end = System.nanoTime();

        if (cycles >= 10000) {
            return 10.0;
        }

        Double reboundTimeMicroSeconds = (Double)Math.ceil((end - start) / 1000.0);
        Double distance = reboundTimeMicroSeconds / 1000000 * SPEED_OF_SOUND / 2;
        return distance;
    }
}
