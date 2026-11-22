package com.sunbird.serve.volunteering.usermanagement.services;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Fetches every record of an RC entity, working around RC's hard limit of 2000 results per list/search call.
 *
 * RC caps every response at 2000 records (also for filtered searches) and ignores paging beyond that.
 * This class searches by osCreatedAt date windows instead. A window that may hold windowSize records or more is
 * split in half until each window is small enough to be returned completely. Results are merged by id.
 *
 * Observed RC behaviour this relies on (checked on prod RC):
 * - osCreatedAt "between" [from, to] includes from and excludes to, so adjacent windows neither overlap nor leave gaps.
 * - A search with offset = windowSize-1, limit = windowSize returns a record only if the window holds
 *   windowSize records or more. This works for both "limit = page size" and "limit = end position" paging.
 *
 * Records without osCreatedAt are not returned by date searches.
 *
 * Safety: at most maxRequests calls per fetch. If the limit is reached the fetch fails with an exception,
 * so callers keep their previous data instead of using a partial result, and a bug cannot flood RC.
 */
@Component
@Slf4j
public class RcWindowedFetcher {

    private static final int RC_MAX_RESULTS = 2000;
    private static final DateTimeFormatter RC_TS =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    private final WebClient rcClient;
    private final Instant start;
    private final int windowSize;
    private final int maxRequests;

    public RcWindowedFetcher(@Qualifier("rcClient") WebClient rcClient,
                             @Value("${rc.fetch.start:2023-08-01T00:00:00Z}") String start,
                             @Value("${rc.fetch.window-size:2000}") int windowSize,
                             @Value("${rc.fetch.max-requests:200}") int maxRequests) {
        this.rcClient = rcClient;
        this.start = Instant.parse(start);
        this.windowSize = Math.max(2, Math.min(windowSize, RC_MAX_RESULTS));
        this.maxRequests = Math.max(1, maxRequests);
        if (this.windowSize != windowSize) {
            log.warn("rc.fetch.window-size {} is outside 2..{}, using {}", windowSize, RC_MAX_RESULTS, this.windowSize);
        }
    }

    /**
     * Returns all records of the entity created from rc.fetch.start until tomorrow, de-duplicated by id.
     *
     * @throws IllegalStateException if the request limit is reached (the result would be incomplete)
     */
    public <T> List<T> fetchAll(String entity, Class<T> type, Function<T, String> idOf) {
        Instant end = Instant.now().plus(1, ChronoUnit.DAYS);
        Map<String, T> byId = new LinkedHashMap<>();
        Deque<Instant[]> pending = new ArrayDeque<>();
        pending.push(new Instant[]{start, end});

        int[] requests = {0};
        int splits = 0;
        int windows = 0;
        int withoutId = 0;
        long startedAt = System.currentTimeMillis();

        while (!pending.isEmpty()) {
            Instant[] window = pending.pop();
            Instant from = window[0];
            Instant to = window[1];

            boolean full = !search(entity, type, from, to, windowSize - 1, windowSize, requests).isEmpty();
            long spanMs = Duration.between(from, to).toMillis();

            if (full && spanMs > 1) {
                Instant mid = from.plusMillis(spanMs / 2);
                // push the later half first so the earlier half is processed first
                pending.push(new Instant[]{mid, to});
                pending.push(new Instant[]{from, mid});
                splits++;
                continue;
            }

            List<T> page = search(entity, type, from, to, 0, windowSize, requests);
            windows++;
            if (full) {
                log.error("RC {} window {} .. {} still holds {} or more records at 1 ms width; some may be missing",
                        entity, RC_TS.format(from), RC_TS.format(to), windowSize);
            }
            for (T item : page) {
                String id = idOf.apply(item);
                if (id == null) {
                    withoutId++;
                } else {
                    byId.putIfAbsent(id, item);
                }
            }
        }

        if (withoutId > 0) {
            log.warn("RC {} fetch: {} record(s) without id were skipped", entity, withoutId);
        }
        log.info("Fetched {} {} from RC in {} request(s): {} window(s), {} split(s), {} ms",
                byId.size(), entity, requests[0], windows, splits, System.currentTimeMillis() - startedAt);
        return new ArrayList<>(byId.values());
    }

    private <T> List<T> search(String entity, Class<T> type, Instant from, Instant to,
                               int offset, int limit, int[] requests) {
        if (++requests[0] > maxRequests) {
            throw new IllegalStateException("RC " + entity + " fetch stopped after " + maxRequests
                    + " requests (rc.fetch.max-requests); result would be incomplete");
        }
        Map<String, Object> body = Map.of(
                "offset", offset,
                "limit", limit,
                "filters", Map.of("osCreatedAt", Map.of("between", List.of(RC_TS.format(from), RC_TS.format(to)))));

        List<T> result = rcClient.post()
                .uri("/" + entity + "/search")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(body)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToFlux(type)
                .collectList()
                .block();
        return result != null ? result : List.of();
    }
}
