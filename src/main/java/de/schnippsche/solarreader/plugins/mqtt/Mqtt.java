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
import de.schnippsche.solarreader.backend.exporter.ExporterInterface;
import de.schnippsche.solarreader.backend.exporter.TransferData;
import de.schnippsche.solarreader.backend.provider.ProviderInterface;
import de.schnippsche.solarreader.backend.provider.ProviderProperty;
import de.schnippsche.solarreader.backend.table.Table;
import de.schnippsche.solarreader.backend.util.Setting;
import de.schnippsche.solarreader.database.Activity;
import de.schnippsche.solarreader.database.ExporterData;
import de.schnippsche.solarreader.database.ProviderData;
import de.schnippsche.solarreader.frontend.ui.*;
import java.io.IOException;
import java.util.*;

/**
 * MQTT plugin that implements both {@link ExporterInterface} and {@link ProviderInterface}.
 * <p>
 * This class manages MQTT communication for both exporting data to a broker and consuming data
 * from a specified topic. It supports dynamic configuration updates, automatic reconnections,
 * and uses a dedicated consumer thread to manage data export asynchronously.
 */
public class Mqtt implements ExporterInterface, ProviderInterface {
  protected static final String TOPIC_NAME = "topic_name";
  protected static final String REQUIRED_ERROR = "mqtt.required.error";
  private final MqttExporter exporter;
  private final MqttProvider provider;
  private ResourceBundle resourceBundle;
  private Locale locale;

  /**
   * Default constructor initializing with {@link MqttConnectionFactory}.
   * <p>
   * Prepares the MQTT plugin using default connection configuration.
   */
  public Mqtt() {
    this(new MqttConnectionFactory());
  }

  /**
   * Constructor allowing injection of a custom MQTT connection factory.
   *
   * @param connectionFactory factory to create MQTT connections.
   */
  public Mqtt(ConnectionFactory<MqttConnection> connectionFactory) {
    setCurrentLocale(Locale.getDefault());
    this.exporter = new MqttExporter(connectionFactory);
    this.provider = new MqttProvider(connectionFactory);
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
  public void setCurrentLocale(Locale locale) {
    this.locale = locale;
    this.resourceBundle = getPluginResourceBundle();
  }

  @Override
  public ProviderData getProviderData() {
    return provider.getProviderData();
  }

  @Override
  public void setProviderData(ProviderData providerData) {
    provider.setProviderData(providerData);
  }

  @Override
  public Activity getDefaultActivity() {
    return provider.getDefaultActivity();
  }

  @Override
  public void configurationHasChanged() {
    provider.configurationHasChanged();
  }

  @Override
  public Optional<UIList> getProviderDialog() {
    return getUiList();
  }

  @Override
  public Optional<List<ProviderProperty>> getSupportedProperties() {
    return provider.getSupportedProperties();
  }

  @Override
  public Optional<List<Table>> getDefaultTables() {
    return provider.getDefaultTables();
  }

  @Override
  public Setting getDefaultProviderSetting() {
    return provider.getDefaultProviderSetting();
  }

  @Override
  public String testProviderConnection(Setting testSetting) throws IOException {
    return provider.testProviderConnection(testSetting);
  }

  @Override
  public void doOnFirstRun() throws IOException {
    provider.doOnFirstRun();
  }

  @Override
  public boolean doActivityWork(Map<String, Object> variables) {
    return provider.doActivityWork(variables);
  }

  @Override
  public ExporterData getExporterData() {
    return exporter.getExporterData();
  }

  @Override
  public void setExporterData(ExporterData exporterData) {
    exporter.setExporterData(exporterData);
  }

  /**
   * Initializes the MQTT exporter by starting the background consumer thread.
   */
  @Override
  public void initialize() {
    exporter.initialize();
  }

  /**
   * Gracefully shuts down MQTT exporter and releases resources.
   */
  @Override
  public void shutdown() {
    provider.shutdown();
    exporter.shutdown();
  }

  /**
   * Adds data to the export queue for asynchronous processing.
   *
   * @param transferData the data to export.
   */
  @Override
  public void addExport(TransferData transferData) {
    exporter.addExport(transferData);
  }

  /**
   * Tests connectivity to the MQTT broker using the given settings.
   *
   * @param testSetting MQTT configuration settings.
   * @return an empty string if successful, or throws IOException on failure.
   */
  @Override
  public String testExporterConnection(Setting testSetting) throws IOException {
    return exporter.testExporterConnection(testSetting);
  }

  /**
   * Retrieves the exporter dialog for the current locale.
   *
   * @return an optional UIList containing the dialog elements.
   */
  @Override
  public Optional<UIList> getExporterDialog() {
    return getUiList();
  }

  /**
   * Retrieves the default exporter configuration.
   *
   * @return a map containing the default configuration parameters.
   */
  @Override
  public Setting getDefaultExporterSetting() {
    return exporter.getDefaultExporterSetting();
  }

  @Override
  public String getLockObject() {
    return provider.getLockObject();
  }

  private Optional<UIList> getUiList() {
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
            .withTooltip(resourceBundle.getString("mqtt.topic.name.tooltip"))
            .withLabel(resourceBundle.getString("mqtt.topic.name.text"))
            .withPlaceholder(resourceBundle.getString("mqtt.topic.name.text"))
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
}
