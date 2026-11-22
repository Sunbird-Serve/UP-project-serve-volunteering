package com.sunbird.serve.volunteering.usermanagement.services;

import com.sunbird.serve.volunteering.models.response.User;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * In-memory cache for user lookups by mobile and email.
 * Refreshes periodically from the registry to avoid fetching all users on every login.
 * 
 * Future: Replace ConcurrentHashMap with RedisTemplate for horizontal scaling.
 */
@Service
@Slf4j
public class UserCacheService {

    private final RcService rcService;

    // Mobile number -> User
    private volatile Map<String, User> mobileIndex = new ConcurrentHashMap<>();
    // Email (lowercase) -> User
    private volatile Map<String, User> emailIndex = new ConcurrentHashMap<>();
    // All users list (for status/agency filtering)
    private volatile List<User> allUsersCache = Collections.emptyList();
    // True once at least one refresh from the registry has succeeded
    private volatile boolean loaded = false;
    // Emails of users created recently that a refresh may not contain yet (email -> created time)
    private final Map<String, Long> pendingEmails = new ConcurrentHashMap<>();
    private static final long PENDING_TTL_MS = 10 * 60 * 1000L;

    @Autowired
    public UserCacheService(RcService rcService) {
        this.rcService = rcService;
    }

    @PostConstruct
    public void init() {
        // Run initial cache load in background so app starts quickly
        new Thread(() -> {
            try {
                Thread.sleep(5000); // wait 5s for app to fully start
                refreshCache();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "cache-init").start();
    }

    /**
     * Refresh cache every 60 seconds.
     * Configurable via application.properties: user.cache.refresh-interval-ms
     */
    @Scheduled(fixedDelayString = "${user.cache.refresh-interval-ms:60000}", initialDelay = 10000)
    public void refreshCache() {
        try {
            log.info("Refreshing user cache...");
            List<User> users = rcService.getAllUsers();

            if (users == null || users.isEmpty()) {
                log.warn("No users returned from registry, keeping existing cache");
                return;
            }

            Map<String, User> newMobileIndex = new ConcurrentHashMap<>();
            Map<String, User> newEmailIndex = new ConcurrentHashMap<>();

            for (User user : users) {
                if (user.getContactDetails() != null) {
                    String mobile = user.getContactDetails().getMobile();
                    String email = user.getContactDetails().getEmail();

                    if (mobile != null && !mobile.isBlank()) {
                        newMobileIndex.put(mobile, user);
                    }
                    if (email != null && !email.isBlank()) {
                        newEmailIndex.put(email.toLowerCase(), user);
                    }
                }
            }

            // Atomic swap
            this.mobileIndex = newMobileIndex;
            this.emailIndex = newEmailIndex;
            this.allUsersCache = users;
            this.loaded = true;

            // Pending emails are no longer needed once the refresh contains them (or they are old enough)
            long cutoff = System.currentTimeMillis() - PENDING_TTL_MS;
            pendingEmails.entrySet().removeIf(e -> e.getValue() < cutoff || newEmailIndex.containsKey(e.getKey()));

            log.info("User cache refreshed: {} users, {} mobile entries, {} email entries",
                    users.size(), newMobileIndex.size(), newEmailIndex.size());
        } catch (Exception e) {
            log.error("Failed to refresh user cache: {}", e.getMessage(), e);
            // Keep existing cache on failure
        }
    }

    /**
     * O(1) lookup by mobile number.
     */
    public Optional<User> getUserByMobile(String mobile) {
        if (mobile == null) return Optional.empty();
        return Optional.ofNullable(mobileIndex.get(mobile));
    }

    /**
     * O(1) lookup by email (case-insensitive).
     */
    public Optional<User> getUserByEmail(String email) {
        if (email == null) return Optional.empty();
        return Optional.ofNullable(emailIndex.get(email.toLowerCase()));
    }

    /**
     * Returns the cached list of all users.
     */
    public List<User> getAllUsers() {
        return allUsersCache;
    }

    /**
     * True once the cache has been filled from the registry at least once.
     * Duplicate checks are only trustworthy after that.
     */
    public boolean isLoaded() {
        return loaded;
    }

    /**
     * True if a user with this email is in the cache, or was created recently and
     * has not appeared in a refresh yet. Case-insensitive.
     */
    public boolean emailExists(String email) {
        if (email == null) return false;
        String key = email.trim().toLowerCase();
        return emailIndex.containsKey(key) || pendingEmails.containsKey(key);
    }

    /**
     * Remember an email immediately after a user is created, so a second request
     * for the same email is rejected before the next refresh picks the user up.
     */
    public void registerEmail(String email) {
        if (email == null || email.isBlank()) return;
        pendingEmails.put(email.trim().toLowerCase(), System.currentTimeMillis());
    }

    /**
     * Remove a user from the cache right away (used after delete) so the email can be reused.
     */
    public void evict(String osid, String email) {
        if (osid != null) {
            emailIndex.values().removeIf(u -> osid.equals(u.getOsid()));
            mobileIndex.values().removeIf(u -> osid.equals(u.getOsid()));
            this.allUsersCache = allUsersCache.stream()
                    .filter(u -> !osid.equals(u.getOsid()))
                    .collect(Collectors.toList());
        }
        if (email != null) {
            pendingEmails.remove(email.trim().toLowerCase());
        }
    }

    /**
     * Schedule a cache refresh in the background (non-blocking).
     * Called after user creation so new users are findable on next lookup.
     */
    public void invalidate() {
        new Thread(this::refreshCache, "cache-invalidate").start();
    }
}
