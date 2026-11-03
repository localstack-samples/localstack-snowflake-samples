package com.example.claims;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The {@code IS_VALID_ICD10} SQL UDF, called directly. The REST tests cover how the
 * service uses it; these pin the ICD-10-CM structure rules themselves.
 */
class Icd10FunctionTest extends IntegrationTestSupport {

    @ParameterizedTest
    @ValueSource(strings = {
        "I10",       // essential hypertension: category only
        "E11.9",     // type 2 diabetes without complications
        "J45.909",   // unspecified asthma, uncomplicated
        "S93.401A",  // ankle sprain, initial encounter: 7 characters with extension
        "U07.1",     // COVID-19 (special-purpose chapter U)
        "C4A.9",     // Merkel cell carcinoma: letter in position 3
    })
    void acceptsCanonicalCodes(String code) {
        assertThat(isValid(code)).as(code).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "",           // empty
        "E1",         // too short
        "E11.",       // dot without subcategory
        "E119",       // undotted form is not canonical
        "e11.9",      // lower case is not canonical
        " E11.9",     // untrimmed
        "1E1.9",      // chapter must be a letter
        "EE1.9",      // second character must be a digit
        "S93.401AB",  // more than 7 characters
        "E11-9",      // wrong separator
    })
    void rejectsMalformedCodes(String code) {
        assertThat(isValid(code)).as(code).isFalse();
    }

    @Test
    void nullIsNotValid() {
        assertThat(isValid(null)).isFalse();
    }

    private Boolean isValid(String code) {
        return jdbc.queryForObject("SELECT IS_VALID_ICD10(?::VARCHAR)", Boolean.class, code);
    }
}
