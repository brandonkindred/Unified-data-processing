package com.unifieddataprocessing.pubsub.msk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.unifieddataprocessing.pubsub.kafka.KafkaConsumerConfig;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.junit.jupiter.api.Test;

class MskConsumerConfigTest {

  @Test
  void iamAuthEmitsExpectedSaslProperties() {
    KafkaConsumerConfig config =
        MskConsumerConfig.iamAuth(
            "b-1.cluster.kafka.us-east-1.amazonaws.com:9098", "test-group", "us-east-1");

    Properties props = config.toProperties();

    assertEquals(
        "b-1.cluster.kafka.us-east-1.amazonaws.com:9098",
        props.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals("test-group", props.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
    assertEquals("SASL_SSL", props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    assertEquals("AWS_MSK_IAM", props.getProperty(SaslConfigs.SASL_MECHANISM));
    assertEquals(
        "software.amazon.msk.auth.iam.IAMLoginModule required awsRegion=\"us-east-1\";",
        props.getProperty(SaslConfigs.SASL_JAAS_CONFIG));
    assertEquals(
        "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
        props.getProperty(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS));
  }

  @Test
  void saslScramEmitsExpectedScramProperties() {
    KafkaConsumerConfig config =
        MskConsumerConfig.saslScram("broker:9096", "test-group", "fake-user", "fake-password");

    Properties props = config.toProperties();

    assertEquals("broker:9096", props.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals("test-group", props.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
    assertEquals("SASL_SSL", props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    assertEquals("SCRAM-SHA-512", props.getProperty(SaslConfigs.SASL_MECHANISM));
    assertEquals(
        "org.apache.kafka.common.security.scram.ScramLoginModule required "
            + "username=\"fake-user\" password=\"fake-password\";",
        props.getProperty(SaslConfigs.SASL_JAAS_CONFIG));
  }

  @Test
  void callerExtrasAreMergedAndCanOverrideDefaults() {
    KafkaConsumerConfig config =
        MskConsumerConfig.iamAuth(
            "broker:9098",
            "test-group",
            "us-west-2",
            Map.of(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT"));

    Properties props = config.toProperties();

    assertEquals("earliest", props.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    assertEquals(
        "SASL_PLAINTEXT", props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    assertEquals("AWS_MSK_IAM", props.getProperty(SaslConfigs.SASL_MECHANISM));
  }

  @Test
  void nullArgumentsAreRejected() {
    assertThrows(
        NullPointerException.class, () -> MskConsumerConfig.iamAuth(null, "g", "us-east-1"));
    assertThrows(
        NullPointerException.class, () -> MskConsumerConfig.iamAuth("b", null, "us-east-1"));
    assertThrows(NullPointerException.class, () -> MskConsumerConfig.iamAuth("b", "g", null));
    assertThrows(
        NullPointerException.class, () -> MskConsumerConfig.saslScram(null, "g", "u", "p"));
    assertThrows(
        NullPointerException.class, () -> MskConsumerConfig.saslScram("b", null, "u", "p"));
    assertThrows(
        NullPointerException.class, () -> MskConsumerConfig.saslScram("b", "g", null, "p"));
    assertThrows(
        NullPointerException.class, () -> MskConsumerConfig.saslScram("b", "g", "u", null));
  }

  @Test
  void quotedRegionIsRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> MskConsumerConfig.iamAuth("b", "g", "us-east-1\" injected=\"x"));
  }

  @Test
  void quotedScramCredentialsAreRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> MskConsumerConfig.saslScram("b", "g", "user\"name", "fake-password"));
    assertThrows(
        IllegalArgumentException.class,
        () -> MskConsumerConfig.saslScram("b", "g", "fake-user", "fake\"password"));
  }
}
