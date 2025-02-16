package com.kafka.training.app;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Person {
    private Long id;
    private String firstName;
    private CountryCode nationality;

    public enum CountryCode {
        FR, DE, ES, IT, GB, US, BE
    }
}
