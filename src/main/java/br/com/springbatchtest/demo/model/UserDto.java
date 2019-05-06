package br.com.springbatchtest.demo.model;

import lombok.*;

import java.time.LocalDate;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UserDto {
    private Long id;
    private String name;
    private Integer salary;
    private LocalDate time;
}
