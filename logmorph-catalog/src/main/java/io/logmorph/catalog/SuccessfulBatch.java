
package io.logmorph.catalog;

import io.logmorph.model.TypedLog;

import java.time.Instant;
import java.util.List;

public record SuccessfulBatch(
        List<TypedLog> logs,
        Instant observedAt
) {}
