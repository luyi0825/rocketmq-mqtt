package org.apache.rocketmq.mqtt.cs.starter;

import org.apache.rocketmq.mqtt.common.facade.SubscriptionPersistManager;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class DefaultSubscriptionPersistManager implements SubscriptionPersistManager {

    Map<String, Set<Subscription>> clientSubscriptions = new ConcurrentHashMap<>();

    Map<String, Set<String>> topicClientIds = new ConcurrentHashMap<>();


    @Override
    public CompletableFuture<Set<Subscription>> loadSubscriptions(String clientId) {
        return CompletableFuture.supplyAsync(() -> clientSubscriptions.get(clientId));

    }

    @Override
    public CompletableFuture<Set<String>> loadSubscribers(String topic) {
        return CompletableFuture.supplyAsync(() -> topicClientIds.get(topic));
    }

    @Override
    public void saveSubscriptions(String clientId, Set<Subscription> subscriptions) {
        clientSubscriptions.put(clientId, subscriptions);
    }

    @Override
    public void saveSubscribers(String topic, Set<String> clientIds) {
        topicClientIds.putIfAbsent(topic, clientIds);
    }

    @Override
    public void removeSubscriptions(String clientId, Set<Subscription> subscriptions) {
        clientSubscriptions.remove(clientId, subscriptions);
    }

    @Override
    public void removeSubscribers(String topic, Set<String> clientIds) {
        Set<String> allClientIds = topicClientIds.get(topic);
        allClientIds.removeAll(clientIds);
    }
}
