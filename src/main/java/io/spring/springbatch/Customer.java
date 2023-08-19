package io.spring.springbatch;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
public class Customer {

    private long id;
    private String firstName;
    private String lastName;
    private String birthdate;

}