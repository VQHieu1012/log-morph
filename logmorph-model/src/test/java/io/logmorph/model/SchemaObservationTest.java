package io.logmorph.model;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class SchemaObservationTest {

    @Test
    void shouldCreateValidObservation() {
        var observation = new SchemaObservation(
                "user_id", ValueType.STRING
        );

        assertEquals("user_id", observation.fieldName());
        assertEquals(ValueType.STRING, observation.valueType());
    }

    @Test
    void shouldRejectInvalidArguments() {
        assertThrows(
                NullPointerException.class,
                () -> new SchemaObservation(null, ValueType.STRING)
        );

        assertThrows(
                NullPointerException.class,
                () -> new SchemaObservation("user_id", null)
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new SchemaObservation(" ", ValueType.STRING)
        );
    }

    @Test
    void shouldDeduplicateEqualObservation() {
        var first = new SchemaObservation("user_id", ValueType.STRING);
        var second = new SchemaObservation("user_id", ValueType.STRING);
        var third = new SchemaObservation("user_id", ValueType.NUMBER);

        Set<SchemaObservation> observations = Set.of(first, third);

        System.out.println(first == second);
        System.out.println(first.equals(second));

        assertEquals(first, second);
        assertTrue(observations.contains(second));
        assertEquals(2, observations.size());
    }


}
