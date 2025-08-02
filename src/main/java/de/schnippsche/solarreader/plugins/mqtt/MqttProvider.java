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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import de.schnippsche.solarreader.backend.connection.general.ConnectionFactory;
import de.schnippsche.solarreader.backend.connection.mqtt.MqttConnection;
import de.schnippsche.solarreader.backend.provider.ProviderProperty;
import de.schnippsche.solarreader.backend.singleton.GlobalGson;
import de.schnippsche.solarreader.backend.table.Table;
import de.schnippsche.solarreader.backend.util.Setting;
import de.schnippsche.solarreader.backend.util.TimeEvent;
import de.schnippsche.solarreader.database.Activity;
import de.schnippsche.solarreader.database.ProviderData;
import de.schnippsche.solarreader.frontend.ui.ValueText;
import java.io.IOException;
import java.net.ConnectException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.tinylog.Logger;

/**
 * {@code MqttProvider} is responsible for connecting to an MQTT broker, subscribing to a configured topic,
 * receiving messages, and storing them in a queue for further processing. It uses a {@link ConnectionFactory}
 * to create MQTT connections and implements {@link MqttCallback} to handle MQTT events.
 *
 * <p>This class supports configuration via {@link ProviderData} and dynamically adapts to configuration changes.
 * It is designed to be thread-safe and integrates with external systems via message payload extraction
 * and variable updates.
 */
public class MqttProvider implements MqttCallback {
  private final ConnectionFactory<MqttConnection> connectionFactory;
  private final ConcurrentLinkedQueue<ValueText> messageQueue;
  protected ProviderData providerData;
  private MqttConnection connection;
  private String topic;

  /**
   * Constructs a new {@code MqttProvider} with a specified connection factory.
   *
   * @param connectionFactory the {@link ConnectionFactory} used to create connections.
   */
  public MqttProvider(ConnectionFactory<MqttConnection> connectionFactory) {
    super();
    this.connectionFactory = connectionFactory; // Initializes the connection factory for MQTT
    topic = "";
    messageQueue = new ConcurrentLinkedQueue<>();
    Logger.debug("instantiate {}", this.getClass().getName());
  }

  /**
   * Retrieves the current {@link ProviderData}, including settings and configuration.
   *
   * @return the current {@link ProviderData}.
   */
  public ProviderData getProviderData() {
    return providerData;
  }

  /**
   * Sets the provider data and reconfigures the MQTT connection accordingly.
   *
   * @param providerData the new {@link ProviderData} to use.
   */
  public void setProviderData(ProviderData providerData) {
    this.providerData = providerData;
    Setting setting = providerData.getSetting();
    topic = setting.getConfigurationValueAsString(Mqtt.TOPIC_NAME, "");
    connection = connectionFactory.createConnection(setting);
    configurationHasChanged();
  }

  /**
   * Returns the default activity used for time-based execution.
   *
   * @return a default {@link Activity} instance.
   */
  public Activity getDefaultActivity() {
    return new Activity(TimeEvent.TIME, 0, TimeEvent.TIME, 86399, 250, TimeUnit.MILLISECONDS);
  }

  /**
   * Returns the supported provider properties.
   *
   * @return an {@link Optional} of a list of {@link ProviderProperty}, empty by default.
   */
  public Optional<List<ProviderProperty>> getSupportedProperties() {
    return Optional.empty();
  }

  /**
   * Returns the default tables used by the provider.
   *
   * @return an {@link Optional} of a list of {@link Table}, empty by default.
   */
  public Optional<List<Table>> getDefaultTables() {
    return Optional.empty();
  }

  /**
   * Returns the default provider setting used when none is configured.
   *
   * @return a {@link Setting} pre-filled with default MQTT parameters.
   */
  public Setting getDefaultProviderSetting() {
    Setting setting = new Setting();
    setting.setProviderHost("localhost");
    setting.setProviderPort(1883);
    setting.setReadTimeoutMilliseconds(5000);
    setting.setSsl(false);
    setting.setConfigurationValue(Mqtt.TOPIC_NAME, "solarreader");
    return setting;
  }

  /**
   * Handles changes to the configuration by reconnecting and resubscribing to the configured topic.
   */
  public void configurationHasChanged() {
    Setting setting = providerData.getSetting();
    topic = setting.getConfigurationValueAsString(Mqtt.TOPIC_NAME, "");
    Logger.debug("new configuration: {}", setting);
    try {
      if (connection != null) connection.disconnect();
      connection = connectionFactory.createConnection(setting);
      connection.connect();
      connection.subscribe(topic, this);
    } catch (IOException e) {
      Logger.error(e);
    }
  }

  /**
   * Tests whether a connection can be established using the given settings.
   *
   * @param testSetting the {@link Setting} to test.
   * @return an empty string if the connection is successful.
   * @throws IOException if the connection cannot be established.
   */
  public String testProviderConnection(Setting testSetting) throws IOException {
    MqttConnection testConnection = connectionFactory.createConnection(testSetting);
    try (testConnection) {
      testConnection.checkConnection();
    }
    return "";
  }

  /**
   * Performs setup actions when the provider is run for the first time, such as connecting and subscribing.
   *
   * @throws IOException if the initial connection fails.
   */
  public void doOnFirstRun() throws IOException {
    Logger.debug("do on first run...");
    connection.connect();
    connection.subscribe(topic, this);
  }

  /**
   * Processes the messages in the queue and updates the given variables map.
   *
   * @param variables the map of variables to update.
   * @return {@code true} if any messages were processed; {@code false} otherwise.
   */
  public boolean doActivityWork(Map<String, Object> variables) {
    if (messageQueue.isEmpty()) return false;
    Iterator<ValueText> iterator = messageQueue.iterator();
    while (iterator.hasNext()) {
      ValueText valueText = iterator.next();
      variables.put(valueText.getValue(), valueText.getText());
      iterator.remove();
    }
    return true;
  }

  /**
   * Shuts down the provider by disconnecting from the MQTT broker.
   */
  public void shutdown() {
    if (connection != null) connection.disconnect();
  }

  /**
   * Retrieves the synchronization lock object from the provider setting.
   *
   * @return the lock object as a {@link String}.
   */
  public String getLockObject() {
    return providerData.getSetting().getLockObject();
  }

  /**
   * Handles the event when the MQTT connection is lost and tries to reconnect.
   *
   * @param cause the exception that caused the connection loss.
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
   * Called when a new MQTT message arrives. Extracts the payload and adds it to the message queue.
   *
   * @param topic       the topic the message was received on.
   * @param mqttMessage the message received.
   */
  @Override
  public void messageArrived(String topic, MqttMessage mqttMessage) {
    String payload = extractPayloadValue(mqttMessage);
    messageQueue.offer(new ValueText(topic, payload));
    Logger.debug(
        "message received, topic='{}', payload='{}', payload value='{}",
        topic,
        new String(mqttMessage.getPayload()),
        payload);
  }

  /**
   * Called when message delivery is complete. Not used in this implementation.
   *
   * @param token the delivery token associated with the message.
   */
  @Override
  public void deliveryComplete(IMqttDeliveryToken token) {
    // Nothing to do
  }

  /**
   * Converts the MQTT message payload into a string value. If the payload is a JSON object
   * containing a "value" field, that value is extracted; otherwise, the raw payload string is returned.
   *
   * @param mqttMessage the incoming {@link MqttMessage}.
   * @return the extracted value or raw payload as a {@link String}.
   */
  private String extractPayloadValue(MqttMessage mqttMessage) {
    String raw = new String(mqttMessage.getPayload());
    try {
      JsonElement element = GlobalGson.getInstance().fromJson(raw, JsonElement.class);
      if (element.isJsonObject()) {
        JsonObject obj = element.getAsJsonObject();
        if (obj.has("value")) {
          return obj.get("value").getAsString();
        }
      }
    } catch (JsonSyntaxException e) {
      // Not a JSON string, fallback to raw
    }

    return raw;
  }
}
