package ifsc.edu.programacaodistribuidaeconcorrente;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * OR-Set (Observe-Remove Set) implementation for distributed systems.
 * This is a CRDT (Conflict-free Replicated Data Type) that supports
 * concurrent additions and removals across multiple devices/replicas.
 */
public class ORSet {
    private final Map<String, Set<UUID>> elements = new ConcurrentHashMap<>();
    
    /**
     * Adds a tag for a specific device.
     * @param deviceId the device identifier
     * @param tag the unique tag to add
     */
    public synchronized void add(String deviceId, UUID tag) {
        elements.computeIfAbsent(deviceId, k -> new HashSet<>()).add(tag);
    }
    
    /**
     * Removes a tag for a specific device.
     * @param deviceId the device identifier
     * @param tag the tag to remove
     */
    public synchronized void remove(String deviceId, UUID tag) {
        Set<UUID> tags = elements.get(deviceId);
        if (tags != null) {
            tags.remove(tag);
            if (tags.isEmpty()) {
                elements.remove(deviceId);
            }
        }
    }
    
    /**
     * Gets all device IDs currently in the set.
     * @return a copy of the device IDs
     */
    public synchronized Set<String> getDeviceIds() {
        return new HashSet<>(elements.keySet());
    }
    
    /**
     * Merges another ORSet into this one.
     * This operation is commutative and idempotent.
     * @param other the ORSet to merge
     */
    public synchronized void merge(ORSet other) {
        for (var entry : other.elements.entrySet()) {
            elements.merge(entry.getKey(), entry.getValue(), (a, b) -> {
                Set<UUID> merged = new HashSet<>(a);
                merged.addAll(b);
                return merged;
            });
        }
    }
    
    /**
     * Checks if a device ID is present in the set.
     * @param deviceId the device identifier to check
     * @return true if the device exists in the set
     */
    public synchronized boolean contains(String deviceId) {
        return elements.containsKey(deviceId);
    }
    
    /**
     * Gets the tags for a specific device.
     * @param deviceId the device identifier
     * @return a copy of the tags for the device, or empty set if device not found
     */
    public synchronized Set<UUID> getTagsForDevice(String deviceId) {
        Set<UUID> tags = elements.get(deviceId);
        return tags != null ? new HashSet<>(tags) : new HashSet<>();
    }
    
    /**
     * Gets the total number of devices in the set.
     * @return the number of devices
     */
    public synchronized int size() {
        return elements.size();
    }
    
    /**
     * Checks if the set is empty.
     * @return true if no devices are in the set
     */
    public synchronized boolean isEmpty() {
        return elements.isEmpty();
    }
    
    @Override
    public synchronized String toString() {
        return "ORSet{elements=" + elements + "}";
    }
}