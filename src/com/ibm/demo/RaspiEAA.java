package com.ibm.demo;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;
import com.ibm.iot.analytics.edgesdk.api.AlertFileActionHandler;
import com.ibm.iot.analytics.edgesdk.api.EdgeAnalyticsAgent;
import com.ibm.iot.analytics.edgesdk.api.client.EdgeAnalyticsClient;
import com.ibm.iot.analytics.edgesdk.api.client.EdgeProperties;
import com.ibm.iot.analytics.edgesdk.api.connectors.StringConnector;
import com.ibm.iot.analytics.edgesdk.api.connectors.transformers.string.JsonTransformer;
import com.ibm.iot.analytics.edgesdk.api.device.Event;
import com.ibm.iotf.devicemgmt.DeviceData;
import com.ibm.iotf.devicemgmt.DeviceFirmware;
import com.ibm.iotf.devicemgmt.DeviceInfo;
import com.ibm.iotf.devicemgmt.DeviceMetadata;
import com.ibm.iotf.devicemgmt.DeviceFirmware.FirmwareState;
import com.ibm.iotf.devicemgmt.gateway.ManagedGateway;

public class RaspiEAA {
	  private static final Logger LOGGER = LoggerFactory.getLogger(RaspiEAA.class);

	  public static void main(String[] args) throws Exception {
		String deviceCfg = args[0];
	    Properties props = loadProperties(deviceCfg);
	    String ORG_ID = props.getProperty("Organization-ID");
	    String AUTH_KEY = props.getProperty("API-Key");
	    String AUTH_TOKEN = props.getProperty("API-Token");

	    String APP_ID = props.getProperty("Application-ID");
	    String GATEWAY_ID = props.getProperty("Gateway-ID");
	    String GATEWAY_TYPE = props.getProperty("Gateway-Type");
	    String GATEWAY_AUTH_TOKEN = props.getProperty("Authentication-Token");

	    EdgeProperties edgeProps = new EdgeProperties(ORG_ID, APP_ID, AUTH_KEY, AUTH_TOKEN,
	        GATEWAY_TYPE, GATEWAY_ID, GATEWAY_AUTH_TOKEN);

	    EdgeAnalyticsClient edgeAnalyticsClient = new EdgeAnalyticsClient(edgeProps);
	    EdgeAnalyticsAgent eaa = edgeAnalyticsClient.getAgent();

	    // Start edge analytics agent with the new gateway.
	    eaa.start();

	    // (Optional) Register an alert handler
	    eaa.registerDeviceActionHandler(new AlertFileActionHandler("ABC.CSV"));

	    // Constructor connector and send events
	    final StringConnector connector = new RangeSensor();
	    connector.setTransformer(new JsonTransformer());
	    eaa.deviceData("RangeDeviceId", "RangeDeviceType", "eventType", connector);
	    ManagedGateway managedGW = getManagedGateway(eaa.getMqttAsyncClient(), GATEWAY_TYPE, GATEWAY_ID);
	    managedGW.sendDeviceManageRequest("RangeDeviceType", "RangeDeviceId", 0, true, true);
	    
	    // Publish a event every 5 seconds.
	    int i = 0;
	    while (i++ < 100000) {
	      //Event event = new Event("SampleStringDeviceId", "SampleStringDeviceType", "{\"Hello\": \"World!\"}");
	      //eaa.deviceData(event);
	      //LOGGER.info("Sent message {} successfully to EAA", event);

	      if (i == 10) {
	        LOGGER.info("Creating managed gatewayclient");

	      }
	      Thread.sleep(5000);
	    }
	  }

	  private static Properties loadProperties(String configPath) throws IOException {
	    Properties props = new Properties();
	    props.load(new FileInputStream(configPath));
	    return props;
	  }

	  private static ManagedGateway getManagedGateway(MqttAsyncClient mqttClient, String gatewayType, String gatewayId) throws Exception {
	    /**
	     * To create a DeviceData object, we will need the following objects:
	     *   - DeviceInfo
	     *   - DeviceMetadata
	     *   - DeviceLocation (optional)
	     *   - DiagnosticErrorCode (optional)
	     *   - DiagnosticLog (optional)
	     *   - DeviceFirmware (optional)
	     */
	    DeviceInfo deviceInfo = new DeviceInfo.Builder().
	        serialNumber("389999").
	        build();

	    DeviceFirmware firmware = new DeviceFirmware.Builder().
	        version("1.0").
	        name("testname").
	        state(FirmwareState.IDLE).
	        build();

	    /**
	     * Create a DeviceMetadata object
	     */
	    JsonObject data = new JsonObject();
	    data.addProperty("customField", "customValue");
	    DeviceMetadata metadata = new DeviceMetadata(data);

	    DeviceData deviceData = new DeviceData.Builder().
	             typeId(gatewayType).
	             deviceId(gatewayId).
	             deviceInfo(deviceInfo).
	             deviceFirmware(firmware).
	             metadata(metadata).
	             build();

	    System.out.println("test");
	    return new ManagedGateway(mqttClient, deviceData);
	  }
}
