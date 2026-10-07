package ru.gamehub;

import io.lettuce.core.RedisException;
import jakarta.validation.ConstraintViolationException;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.server.ResponseStatusException;

import static ru.gamehub.ApiModels.ApiError;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<ApiError> status(ResponseStatusException e) {
        return ResponseEntity.status(e.getStatusCode()).body(new ApiError("REQUEST_FAILED", e.getReason()));
    }

    @ExceptionHandler({
            MethodArgumentNotValidException.class,
            ConstraintViolationException.class,
            HttpMessageNotReadableException.class,
            HandlerMethodValidationException.class,
            MethodArgumentTypeMismatchException.class
    })
    public ResponseEntity<ApiError> validation(Exception e) {
        String message = e instanceof MethodArgumentNotValidException validation
                ? validation.getBindingResult().getAllErrors().getFirst().getDefaultMessage()
                : "Invalid JSON, parameter type or validation constraint";
        return ResponseEntity.badRequest().body(new ApiError("VALIDATION_FAILED", message));
    }

    @ExceptionHandler(RedisException.class)
    public ResponseEntity<ApiError> redis(RedisException e) {
        return ResponseEntity.status(503).body(new ApiError("REDIS_UNAVAILABLE",
                "Redis temporarily unavailable. A write may have been applied; do not blindly retry increments. "
                        + e.getMessage()));
    }
}
