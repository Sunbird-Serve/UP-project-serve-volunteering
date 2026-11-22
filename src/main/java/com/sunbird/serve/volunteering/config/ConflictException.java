package com.sunbird.serve.volunteering.config;

/**
 * Thrown when a request conflicts with existing data (duplicate email, duplicate profile,
 * duplicate volunteering hours, or a delete that would orphan records). Mapped to HTTP 409.
 */
public class ConflictException extends RuntimeException {

    public ConflictException(String message) {
        super(message);
    }
}
