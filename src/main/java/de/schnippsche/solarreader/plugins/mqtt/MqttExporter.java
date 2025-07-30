/*
 * Copyright (c) 2024-2025 Stefan Toengi
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package de.schnippsche.solarreader.plugins.mqtt;

import de.schnippsche.solarreader.backend.connection.general.ConnectionFactory;
import de.schnippsche.solarreader.backend.connection.mqtt.MqttConnection;
import de.schnippsche.solarreader.backend.connection.mqtt.PublishEntry;
import de.schnippsche.solarreader.backend.exporter.TransferData;
import de.schnippsche.solarreader.backend.singleton.GlobalUsrStore;
import de.schnippsche.solarreader.backend.util.Setting;
import de.schnippsche.solarreader.database.ExporterData;
import de.schnippsche.solarreader.database.ProviderData;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.ConnectException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.tinylog.Logger;

/**
 * {@code MqttExporter} is responsible for exporting data to an MQTT broker.
 * It establishes a connection using a configurable {@link ConnectionFactory},
 * listens to export requests via a blocking queue, and publishes variable data
 * as MQTT messages. The class supports dynamic configuration updates and
 * ensures reliable message delivery through a dedicated background thread.
 *
 * <p>Implements {@link MqttCallback} to handle MQTT connection callbacks.
 */
public class MqttExporter implements MqttCallback {
  private final ConnectionFactory<MqttConnection> connectionFactory;
  private final BlockingQueue<TransferData> queue;
  private MqttConnection connection;
  private String host;
  private String topic;
  private Thread consumerThread;
  private volatile boolean running = true;
  private ExporterData exporterData;

  /**
   * Constructs a new {@code MqttExporter} with a specified {@link ConnectionFactory}.
   *
   * @param connectionFactory the factory to use for MQTT connections.
   */
  public MqttExporter(ConnectionFactory<MqttConnection> connectionFactory) {
    super();
    this.connectionFactory = connectionFactory;
    this.queue = new LinkedBlockingQueue<>();
  }

  /**
   * Initializes the exporter and starts the background thread for processing export data.
   */
  public void initialize() {
    Logger.debug("initialize mqtt exporter");
    consumerThread = new Thread(this::processQueue);
    consumerThread.setName("MQTTExporterThread");
    consumerThread.start();
  }

  /**
   * Shuts down the exporter by stopping the background thread and disconnecting from the MQTT broker.
   */
  public void shutdown() {
    running = false;
    consumerThread.interrupt();
    if (connection != null) connection.disconnect();
  }

  /**
   * Adds an export data entry to the queue for processing and publishing.
   *
   * @param transferData the data to be exported.
   */
  public void addExport(TransferData transferData) {

    if (transferData.getVariables().isEmpty()) {
      Logger.debug("no exporting variables, skip export");
      return;
    }
    Logger.debug(
        "add {} entries for broker '{}'",
        transferData.getVariables().size(),
        exporterData.getName());
    exporterData.setLastCall(transferData.getTimestamp());
    queue.add(transferData);
  }

  /**
   * Returns the current exporter configuration and metadata.
   *
   * @return the {@link ExporterData} instance.
   */
  public ExporterData getExporterData() {
    return exporterData;
  }

  /**
   * Sets the exporter data and updates the MQTT configuration accordingly.
   *
   * @param exporterData the new {@link ExporterData} to apply.
   */
  public void setExporterData(ExporterData exporterData) {
    this.exporterData = exporterData;
    updateConfiguration();
  }

  /**
   * Tests the MQTT connection using the specified setting.
   *
   * @param testSetting the configuration to test.
   * @return an empty string if successful.
   * @throws IOException if the connection test fails.
   */
  public String testExporterConnection(Setting testSetting) throws IOException {
    try (MqttConnection testConnection = connectionFactory.createConnection(testSetting)) {
      testConnection.checkConnection();
    }
    return "";
  }

  /**
   * Provides a default configuration for the MQTT exporter.
   *
   * @return a {@link Setting} pre-configured with common MQTT parameters.
   */
  public Setting getDefaultExporterSetting() {
    Setting setting = new Setting();
    setting.setProviderHost("localhost");
    setting.setProviderPort(1883);
    setting.setReadTimeoutMilliseconds(5000);
    setting.setKeepAliveSeconds(30);
    setting.setSsl(false);
    setting.setConfigurationValue(Mqtt.TOPIC_NAME, "Solarreader");
    return setting;
  }

  /**
   * Called when the MQTT connection is lost. Attempts automatic reconnection.
   *
   * @param cause the reason the connection was lost.
   */
  @Override
  public void connectionLost(Throwable cause) {
    Logger.error("connection lost: {}", cause.getMessage());
    try {
      connection.connect();
    } catch (ConnectException e) {
      Logger.error(e.getMessage());
    }
  }

  /**
   * Not used. Required by {@link MqttCallback}.
   */
  @Override
  public void messageArrived(String topic, MqttMessage mqttMessage) {
    // Nothing to do
  }

  /**
   * Not used. Required by {@link MqttCallback}.
   */
  @Override
  public void deliveryComplete(IMqttDeliveryToken token) {
    // Nothing to do
  }

  /**
   * Updates the MQTT connection and topic from the current configuration.
   */
  public void updateConfiguration() {
    if (connection != null) connection.disconnect();
    Setting setting = exporterData.getSetting();
    host = setting.getProviderHost();
    topic = setting.getConfigurationValueAsString(Mqtt.TOPIC_NAME, "");
    connection = connectionFactory.createConnection(setting);
    connect();
  }

  /**
   * Establishes a connection to the MQTT broker.
   */
  private void connect() {
    try {
      Logger.debug("try connecting to mqtt server '{}' with topic '{}'", host, topic);
      connection.connect();
      Logger.debug("connected to mqtt server '{}'", host, topic);
    } catch (Exception e) {
      Logger.error(e);
    }
  }

  /**
   * Publishes the contents of a {@link TransferData} object to the MQTT broker.
   *
   * @param transferData the data to be published.
   */
  private void doExport(TransferData transferData) {
    if (exporterData.hasConfigurationChanged()) {
      Logger.debug("updated configuration detected...");
      updateConfiguration();
      exporterData.setConfigurationChanged(false);
    }
    if (transferData.getVariables().isEmpty()) {
      Logger.debug("no exporting variables detected...");
      return;
    }
    Logger.debug(
        "export {} entries to '{}' at host '{}' started...",
        transferData.getVariables().size(),
        exporterData.getName(),
        host);

    long startTime = System.currentTimeMillis();
    List<PublishEntry> entries = new ArrayList<>(transferData.getVariables().size());
    long sourceKey = transferData.getSourceKey();
    // Provider or Automation ?
    String providerName = null;
    boolean isProvider = false;
    Logger.debug("search provider with key {}", sourceKey);
    Optional<ProviderData> optionalProviderData =
        GlobalUsrStore.getInstance().getProviderData(sourceKey);

    if (optionalProviderData.isPresent()) {
      providerName = optionalProviderData.get().getName();
      isProvider = true;
      Logger.debug("found provider, name={}", providerName);
    }
    for (Map.Entry<String, Object> entry : transferData.getVariables().entrySet()) {
      String completeTopic =
          isProvider ? generateValidTopic(topic, providerName, entry.getKey()) : entry.getKey();
      Object val = entry.getValue();
      String value =
          val instanceof BigDecimal ? ((BigDecimal) val).toPlainString() : String.valueOf(val);
      entries.add(new PublishEntry(completeTopic, value));
    }
    connection.publish(entries);
    Logger.debug(
        "export to '{}' finished in {} ms",
        exporterData.getName(),
        (System.currentTimeMillis() - startTime));
  }

  /**
   * Continuously consumes and processes items from the export queue in a background thread.
   */
  private void processQueue() {
    while (running) {
      try {
        TransferData entry = queue.take();
        doExport(entry);
      } catch (InterruptedException e) {
        if (!running) {
          break; // Exit loop if not running
        }
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Builds a valid MQTT topic string by combining a prefix, device name, and a variable leaf.
   * Invalid characters are replaced and unnecessary slashes are removed.
   *
   * @param prefix     the topic prefix (can be null or empty).
   * @param deviceName the device name.
   * @param leaf       the variable name or leaf element.
   * @return a sanitized and valid MQTT topic.
   * @throws IllegalArgumentException if the leaf is empty.
   */
  private String generateValidTopic(String prefix, String deviceName, String leaf) {
    // Sanitize and trim the prefix, deviceName, and leaf
    prefix = sanitizeAndTrim(prefix);
    deviceName = sanitizeAndTrim(deviceName);
    leaf = sanitizeAndTrim(leaf);

    // Validate that the leaf is not empty (leaf is required)
    if (leaf.isEmpty()) {
      throw new IllegalArgumentException("Leaf must not be null or empty");
    }

    // Use StringJoiner to build the topic, skipping empty segments
    StringJoiner topicJoiner = new StringJoiner("/");
    if (!prefix.isEmpty()) topicJoiner.add(prefix);
    if (!deviceName.isEmpty()) topicJoiner.add(deviceName);
    topicJoiner.add(leaf); // leaf is always added since it's mandatory

    return topicJoiner.toString();
  }

  /**
   * Sanitizes a string for MQTT topic usage by replacing invalid characters
   * and trimming slashes.
   *
   * @param input the string to sanitize.
   * @return a cleaned string suitable for MQTT topics.
   */
  private String sanitizeAndTrim(String input) {
    return (input == null || input.trim().isEmpty())
        ? ""
        : input.replaceAll("^/+", "").replaceAll("/+$", "").replaceAll("[^a-zA-Z0-9/_-]", "_");
  }
}
