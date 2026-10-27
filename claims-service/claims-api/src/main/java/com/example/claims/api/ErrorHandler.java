package com.example.claims.api;

import com.example.claims.service.ClaimNotAdjudicableException;
import com.example.claims.service.ClaimNotFoundException;
import com.example.claims.service.DuplicateClaimException;
import com.example.claims.service.InvalidClaimException;
import java.net.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * Maps domain and database errors to RFC 9457 problem details. Request validation
 * errors (400, including Spring MVC's built-in method validation) come from
 * {@link ResponseEntityExceptionHandler}.
 */
@RestControllerAdvice
public class ErrorHandler extends ResponseEntityExceptionHandler {

    static final String PROBLEM_BASE = "urn:claims:problem:";

    private static final Logger log = LoggerFactory.getLogger(ErrorHandler.class);

    @ExceptionHandler
    ProblemDetail duplicate(DuplicateClaimException e) {
        ProblemDetail problem = problem(HttpStatus.CONFLICT, "duplicate-claim", "Duplicate claim", e.getMessage());
        problem.setProperty("claimId", e.claimId());
        return problem;
    }

    @ExceptionHandler
    ProblemDetail invalid(InvalidClaimException e) {
        return problem(HttpStatus.UNPROCESSABLE_ENTITY, e.problem().slug(), "Claim rejected", e.getMessage());
    }

    @ExceptionHandler
    ProblemDetail notFound(ClaimNotFoundException e) {
        ProblemDetail problem = problem(HttpStatus.NOT_FOUND, "claim-not-found", "Claim not found", e.getMessage());
        problem.setProperty("claimId", e.claimId());
        return problem;
    }

    /** The procedure's custom exception: the claim exists but cannot be adjudicated now. */
    @ExceptionHandler
    ProblemDetail notAdjudicable(ClaimNotAdjudicableException e) {
        ProblemDetail problem = problem(HttpStatus.UNPROCESSABLE_ENTITY, "claim-not-pending",
                "Claim cannot be adjudicated", e.getMessage());
        problem.setProperty("claimId", e.claimId());
        problem.setProperty("snowflakeErrorCode", e.snowflakeErrorCode());
        return problem;
    }

    /** Anything else from Snowflake: log it, but never leak SQL or driver details. */
    @ExceptionHandler
    ProblemDetail database(DataAccessException e) {
        log.error("Snowflake call failed", e);
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, "database-error", "Database error",
                "The claims database could not complete the request");
    }

    private static ProblemDetail problem(HttpStatus status, String slug, String title, String detail) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setType(URI.create(PROBLEM_BASE + slug));
        problem.setTitle(title);
        return problem;
    }
}
