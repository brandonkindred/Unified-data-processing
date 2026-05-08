package com.unifieddataprocessing.pubsub.msk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.unifieddataprocessing.pubsub.kafka.KafkaProducerConfig;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.junit.jupiter.api.Test;

class MskPublisherConfigTest {

  @Test
  void iamAuthEmitsExpectedSaslProperties() {
    KafkaProducerConfig config =
        MskPublisherConfig.iamAuth(
            "b-1.cluster.kafka.us-east-1.amazonaws.com:9098", "us-east-1");

    Properties props = config.toProperties();

    assertEquals(
        "b-1.cluster.kafka.us-east-1.amazonaws.com:9098",
        props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
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
    KafkaProducerConfig config =
        MskPublisherConfig.saslScram("broker:9096", "fake-user", "fake-password");

    Properties props = config.toProperties();

    assertEquals("broker:9096", props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals("SASL_SSL", props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    assertEquals("SCRAM-SHA-512", props.getProperty(SaslConfigs.SASL_MECHANISM));
    assertEquals(
        "org.apache.kafka.common.security.scram.ScramLoginModule required "
            + "username=\"fake-user\" password=\"fake-password\";",
        props.getProperty(SaslConfigs.SASL_JAAS_CONFIG));
  }

  @Test
  void callerExtrasAreMergedAndCanOverrideDefaults() {
    KafkaProducerConfig config =
        MskPublisherConfig.iamAuth(
            "broker:9098",
            "us-west-2",
            Map.of(
                ProducerConfig.LINGER_MS_CONFIG, "10",
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT"));

    Properties props = config.toProperties();

    assertEquals("10", props.getProperty(ProducerConfig.LINGER_MS_CONFIG));
    assertEquals(
        "SASL_PLAINTEXT", props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    assertEquals("AWS_MSK_IAM", props.getProperty(SaslConfigs.SASL_MECHANISM));
  }

  @Test
  void nullArgumentsAreRejected() {
    assertThrows(NullPointerException.class, () -> MskPublisherConfig.iamAuth(null, "us-east-1"));
    assertThrows(NullPointerException.class, () -> MskPublisherConfig.iamAuth("b", null));
    assertThrows(NullPointerException.class, () -> MskPublisherConfig.saslScram(null, "u", "p"));
    assertThrows(NullPointerException.class, () -> MskPublisherConfig.saslScram("b", null, "p"));
    assertThrows(NullPointerException.class, () -> MskPublisherConfig.saslScram("b", "u", null));
  }

  @Test
  void quotedRegionIsRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> MskPublisherConfig.iamAuth("b", "us-east-1\" injected=\"x"));
  }

  @Test
  void quotedScramCredentialsAreRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> MskPublisherConfig.saslScram("b", "user\"name", "fake-password"));
    assertThrows(
        IllegalArgumentException.class,
        () -> MskPublisherConfig.saslScram("b", "fake-user", "fake\"password"));
  }
}
