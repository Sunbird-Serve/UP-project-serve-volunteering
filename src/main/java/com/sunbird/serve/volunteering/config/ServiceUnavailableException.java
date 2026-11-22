package com.sunbird.serve.volunteering.config;

/**
 * Thrown when the service cannot safely answer yet (for example the user directory has not loaded).
 * Mapped to HTTP 503.
 */
public class ServiceUnavailableException extends RuntimeException {

    public ServiceUnavailableException(String message) {
        super(message);
    }
}
