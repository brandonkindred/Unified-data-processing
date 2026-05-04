package com.unifieddataprocessing.pubsub.msk;

import com.unifieddataprocessing.pubsub.kafka.KafkaConsumerConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class MskConsumerConfig {

    private static final String IAM_LOGIN_MODULE =
            "software.amazon.msk.auth.iam.IAMLoginModule";
    private static final String IAM_CALLBACK_HANDLER =
            "software.amazon.msk.auth.iam.IAMClientCallbackHandler";
    private static final String SCRAM_LOGIN_MODULE =
            "org.apache.kafka.common.security.scram.ScramLoginModule";

    private MskConsumerConfig() {
    }

    public static KafkaConsumerConfig iamAuth(String bootstrapServers, String groupId, String region) {
        return iamAuth(bootstrapServers, groupId, region, Collections.emptyMap());
    }

    public static KafkaConsumerConfig iamAuth(
            String bootstrapServers,
            String groupId,
            String region,
            Map<String, Object> extras) {
        Objects.requireNonNull(bootstrapServers, "bootstrapServers");
        Objects.requireNonNull(groupId, "groupId");
        Objects.requireNonNull(region, "region");
        validateNoQuotes("region", region);

        String jaasConfig = IAM_LOGIN_MODULE + " required awsRegion=\"" + region + "\";";

        Map<String, Object> merged = new LinkedHashMap<>();
        merged.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        merged.put(SaslConfigs.SASL_MECHANISM, "AWS_MSK_IAM");
        merged.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
        merged.put(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, IAM_CALLBACK_HANDLER);
        if (extras != null) {
            merged.putAll(extras);
        }
        return new KafkaConsumerConfig(bootstrapServers, groupId, merged);
    }

    public static KafkaConsumerConfig saslScram(
            String bootstrapServers,
            String groupId,
            String username,
            String password) {
        return saslScram(bootstrapServers, groupId, username, password, Collections.emptyMap());
    }

    public static KafkaConsumerConfig saslScram(
            String bootstrapServers,
            String groupId,
            String username,
            String password,
            Map<String, Object> extras) {
        Objects.requireNonNull(bootstrapServers, "bootstrapServers");
        Objects.requireNonNull(groupId, "groupId");
        Objects.requireNonNull(username, "username");
        Objects.requireNonNull(password, "password");
        validateNoQuotes("username", username);
        validateNoQuotes("password", password);

        String jaasConfig = SCRAM_LOGIN_MODULE + " required username=\""
                + username + "\" password=\"" + password + "\";";

        Map<String, Object> merged = new LinkedHashMap<>();
        merged.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        merged.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
        merged.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
        if (extras != null) {
            merged.putAll(extras);
        }
        return new KafkaConsumerConfig(bootstrapServers, groupId, merged);
    }

    private static void validateNoQuotes(String name, String value) {
        if (value.indexOf('"') >= 0) {
            throw new IllegalArgumentException(name + " must not contain a double-quote character");
        }
    }
}
