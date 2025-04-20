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
import de.schnippsche.solarreader.backend.connection.mqtt.MqttConnectionFactory;
import de.schnippsche.solarreader.backend.protocol.KnownProtocol;
import de.schnippsche.solarreader.backend.provider.AbstractProvider;
import de.schnippsche.solarreader.backend.provider.ProviderProperty;
import de.schnippsche.solarreader.backend.provider.SupportedInterface;
import de.schnippsche.solarreader.backend.table.Table;
import de.schnippsche.solarreader.backend.util.Setting;
import de.schnippsche.solarreader.backend.util.TimeEvent;
import de.schnippsche.solarreader.database.Activity;
import de.schnippsche.solarreader.database.ProviderData;
import de.schnippsche.solarreader.frontend.ui.HtmlInputType;
import de.schnippsche.solarreader.frontend.ui.HtmlWidth;
import de.schnippsche.solarreader.frontend.ui.UIInputElementBuilder;
import de.schnippsche.solarreader.frontend.ui.UIList;
import de.schnippsche.solarreader.frontend.ui.UITextElementBuilder;
import de.schnippsche.solarreader.frontend.ui.ValueText;
import de.schnippsche.solarreader.plugin.PluginMetadata;
import java.io.IOException;
import java.net.ConnectException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ResourceBundle;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.tinylog.Logger;

/**
 * The {@link Mqtt} class is a provider class for interacting with an MQTT broker. It extends {@link
 * AbstractProvider} and implements the {@link MqttCallback} interface, enabling it to handle MQTT
 * messages and manage the MQTT connection.
 *
 * <p>This class facilitates the communication between the application and an MQTT broker, allowing
 * for receiving incoming messages. The implementation of the {@link MqttCallback} interface allows
 * the class to handle events such as message arrival, connection loss, and other MQTT
 * protocol-related events.
 */
@PluginMetadata(
    name = "MQTT",
    version = "1.0.1",
    author = "Stefan Töngi",
    url = "https://github.com/solarreader-plugins/plugin-Mqtt",
    svgImage = "mqtt.svg",
    supportedInterfaces = {SupportedInterface.NONE},
    usedProtocol = KnownProtocol.HTTP,
    supports = "")
public class Mqtt extends AbstractProvider implements MqttCallback {
  /** the constant topic name */
  protected static final String TOPIC_NAME = "topic_name";

  private static final String REQUIRED_ERROR = "mqtt.required.error";
  private final ConnectionFactory<MqttConnection> connectionFactory;
  private final ConcurrentLinkedQueue<ValueText> messageQueue;
  private MqttConnection connection;
  private String topic;

  /**
   * Constructs a new instance of the {@link Mqtt} class using the default {@link
   * MqttConnectionFactory}. This constructor initializes the MQTT provider with the default
   * connection factory, allowing the application to interact with an MQTT broker. It also
   * initializes the topic as an empty string and sets up a concurrent message queue for handling
   * incoming messages.
   */
  public Mqtt() {
    this(new MqttConnectionFactory());
  }

  /**
   * Constructs a new instance of the {@link Mqtt} class with a custom {@link ConnectionFactory} for
   * managing MQTT connections. This constructor allows you to provide a custom connection factory
   * for creating and managing MQTT connections, offering flexibility in how the connection is
   * established. It also initializes the topic as an empty string and sets up a concurrent message
   * queue for message handling.
   *
   * @param connectionFactory the {@link ConnectionFactory} to use for creating MQTT connections
   */
  public Mqtt(ConnectionFactory<MqttConnection> connectionFactory) {
    super();
    this.connectionFactory = connectionFactory; // Initializes the connection factory for MQTT
    topic = ""; // Default topic is an empty string
    messageQueue =
        new ConcurrentLinkedQueue<>(); // Initializes a thread-safe message queue for storing
    // incoming messages
    Logger.debug("instantiate {}", this.getClass().getName());
  }

  /**
   * Retrieves the resource bundle for the plugin based on the specified locale.
   *
   * <p>This method overrides the default implementation to return a {@link ResourceBundle} for the
   * plugin using the provided locale.
   *
   * @return The {@link ResourceBundle} for the plugin, localized according to the specified locale.
   */
  @Override
  public ResourceBundle getPluginResourceBundle() {
    return ResourceBundle.getBundle("mqtt", locale);
  }

  @Override
  public Activity getDefaultActivity() {
    return new Activity(TimeEvent.TIME, 0, TimeEvent.TIME, 86399, 250, TimeUnit.MILLISECONDS);
  }

  @Override
  public Optional<UIList> getProviderDialog() {
    UIList uiList = new UIList();
    uiList.addElement(
        new UITextElementBuilder().withLabel(resourceBundle.getString("mqtt.title")).build());
    uiList.addElement(
        new UIInputElementBuilder()
            .withName(Setting.PROVIDER_HOST)
            .withType(HtmlInputType.TEXT)
            .withColumnWidth(HtmlWidth.HALF)
            .withRequired(true)
            .withTooltip(resourceBundle.getString("mqtt.host.tooltip"))
            .withLabel(resourceBundle.getString("mqtt.host.text"))
            .withPlaceholder(resourceBundle.getString("mqtt.host.text"))
            .withInvalidFeedback(resourceBundle.getString(REQUIRED_ERROR))
            .build());
    uiList.addElement(
        new UIInputElementBuilder()
            .withName(Setting.PROVIDER_PORT)
            .withType(HtmlInputType.NUMBER)
            .withColumnWidth(HtmlWidth.HALF)
            .withRequired(true)
            .withTooltip(resourceBundle.getString("mqtt.port.tooltip"))
            .withLabel(resourceBundle.getString("mqtt.port.text"))
            .withPlaceholder(resourceBundle.getString("mqtt.port.text"))
            .withInvalidFeedback(resourceBundle.getString(REQUIRED_ERROR))
            .build());

    uiList.addElement(
        new UIInputElementBuilder()
            .withName(Setting.OPTIONAL_USER)
            .withType(HtmlInputType.TEXT)
            .withColumnWidth(HtmlWidth.HALF)
            .withRequired(false)
            .withTooltip(resourceBundle.getString("mqtt.user.tooltip"))
            .withLabel(resourceBundle.getString("mqtt.user.text"))
            .withPlaceholder(resourceBundle.getString("mqtt.user.text"))
            .build());
    uiList.addElement(
        new UIInputElementBuilder()
            .withName(Setting.OPTIONAL_PASSWORD)
            .withType(HtmlInputType.TEXT)
            .withColumnWidth(HtmlWidth.HALF)
            .withRequired(false)
            .withTooltip(resourceBundle.getString("mqtt.password.tooltip"))
            .withLabel(resourceBundle.getString("mqtt.password.text"))
            .withPlaceholder(resourceBundle.getString("mqtt.password.text"))
            .build());
    uiList.addElement(
        new UIInputElementBuilder()
            .withName(TOPIC_NAME)
            .withType(HtmlInputType.TEXT)
            .withColumnWidth(HtmlWidth.HALF)
            .withRequired(true)
            .withTooltip(resourceBundle.getString("mqtt.topicname.tooltip"))
            .withLabel(resourceBundle.getString("mqtt.topicname.text"))
            .withPlaceholder(resourceBundle.getString("mqtt.topicname.text"))
            .withInvalidFeedback(resourceBundle.getString(REQUIRED_ERROR))
            .build());
    uiList.addElement(
        new UIInputElementBuilder()
            .withName(Setting.USE_SSL)
            .withType(HtmlInputType.CHECKBOX)
            .withColumnWidth(HtmlWidth.HALF)
            .withTooltip(resourceBundle.getString("mqtt.use.ssl.tooltip"))
            .withLabel(resourceBundle.getString("mqtt.use.ssl.label"))
            .withPlaceholder(resourceBundle.getString("mqtt.use.ssl.text"))
            .build());

    return Optional.of(uiList);
  }

  @Override
  public Optional<List<ProviderProperty>> getSupportedProperties() {
    return Optional.empty();
  }

  @Override
  public Optional<List<Table>> getDefaultTables() {
    return Optional.empty();
  }

  @Override
  public Setting getDefaultProviderSetting() {
    Setting setting = new Setting();
    setting.setProviderHost("localhost");
    setting.setProviderPort(1883);
    setting.setReadTimeoutMilliseconds(5000);
    setting.setSsl(false);
    setting.setConfigurationValue(TOPIC_NAME, "solarreader");
    return setting;
  }

  @Override
  public void configurationHasChanged() {
    super.configurationHasChanged();
    Setting setting = providerData.getSetting();
    topic = setting.getConfigurationValueAsString(TOPIC_NAME, "");
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

  @Override
  public String testProviderConnection(Setting testSetting) throws IOException {
    MqttConnection testConnection = connectionFactory.createConnection(testSetting);
    try (testConnection) {
      testConnection.checkConnection();
    }
    return "";
  }

  @Override
  public void doOnFirstRun() throws IOException {
    Logger.debug("do on first run...");
    connection.connect();
    connection.subscribe(topic, this);
  }

  @Override
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

  @Override
  public void shutdown() {
    connection.disconnect();
  }

  @Override
  public void setProviderData(ProviderData providerData) {
    this.providerData = providerData;
    Setting setting = providerData.getSetting();
    topic = setting.getConfigurationValueAsString(TOPIC_NAME, "");
    connection = connectionFactory.createConnection(setting);
    configurationHasChanged();
  }

  @Override
  public void connectionLost(Throwable cause) {
    Logger.error("connection lost: {}", cause.getMessage());
    try {
      connection.connect();
    } catch (ConnectException e) {
      Logger.error(e.getMessage());
    }
  }

  @Override
  public void messageArrived(String topic, MqttMessage mqttMessage) {
    String msg = new String(mqttMessage.getPayload());
    messageQueue.offer(new ValueText(topic, msg));
    Logger.debug("message received, topic='{}', message='{}'", topic, msg);
  }

  @Override
  public void deliveryComplete(IMqttDeliveryToken token) {
    // Nothing to do
  }
}
