package br.com.springbatchtest.demo.repository;

import br.com.springbatchtest.demo.model.User;
import br.com.springbatchtest.demo.model.UserProjection;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

public interface UserRepository extends JpaRepository<User, Long>, JpaSpecificationExecutor<User> {

    Page<User> findAll(Specification<User> userSpecification, Pageable pageable);

}
