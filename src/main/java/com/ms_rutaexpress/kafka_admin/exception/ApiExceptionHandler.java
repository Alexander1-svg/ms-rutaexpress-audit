package com.ms_rutaexpress.kafka_admin.exception;

import com.ms_rutaexpress.kafka_admin.dto.ApiErrorResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;

import java.time.Clock;
import java.time.Instant;

@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    private final Clock clock;

    public ApiExceptionHandler(Clock clock) {
        this.clock = clock;
    }

    @ExceptionHandler({
            MethodArgumentNotValidException.class,
            HandlerMethodValidationException.class,
            ConstraintViolationException.class,
            HttpMessageNotReadableException.class,
            InvalidAdminRequestException.class
    })
    public ResponseEntity<ApiErrorResponse> badRequest(Exception exception, HttpServletRequest request) {
        String message = exception instanceof InvalidAdminRequestException
                ? exception.getMessage()
                : "Solicitud inválida";
        return response(HttpStatus.BAD_REQUEST, message, request);
    }

    @ExceptionHandler(TopicNotFoundException.class)
    public ResponseEntity<ApiErrorResponse> notFound(TopicNotFoundException exception, HttpServletRequest request) {
        return response(HttpStatus.NOT_FOUND, exception.getMessage(), request);
    }

    @ExceptionHandler(TopicConflictException.class)
    public ResponseEntity<ApiErrorResponse> conflict(TopicConflictException exception, HttpServletRequest request) {
        return response(HttpStatus.CONFLICT, exception.getMessage(), request);
    }

    @ExceptionHandler(KafkaUnavailableException.class)
    public ResponseEntity<ApiErrorResponse> unavailable(
            KafkaUnavailableException exception,
            HttpServletRequest request
    ) {
        log.warn("Kafka no disponible: errorType={}", rootCauseName(exception));
        return response(HttpStatus.SERVICE_UNAVAILABLE, "Kafka no está disponible", request);
    }

    @ExceptionHandler({KafkaAdminOperationException.class, Exception.class})
    public ResponseEntity<ApiErrorResponse> internalError(Exception exception, HttpServletRequest request) {
        log.error("Error administrativo no controlado: errorType={}", rootCauseName(exception), exception);
        return response(HttpStatus.INTERNAL_SERVER_ERROR, "Error interno", request);
    }

    private ResponseEntity<ApiErrorResponse> response(
            HttpStatus status,
            String message,
            HttpServletRequest request
    ) {
        return ResponseEntity.status(status).body(new ApiErrorResponse(
                Instant.now(clock),
                status.value(),
                status.getReasonPhrase(),
                message,
                request.getRequestURI()
        ));
    }

    private String rootCauseName(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getClass().getSimpleName();
    }
}
