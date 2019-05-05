package br.com.springbatchtest.demo.model;

import org.springframework.data.jpa.domain.Specification;

public class UserSpecification {

    public static Specification<User> dept(String value) {
        return (root, criteriaQuery, criteriaBuilder) ->
                criteriaBuilder.equal(root.get("dept"), value);
    }

}
