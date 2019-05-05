package br.com.springbatchtest.demo.batch.user.exporter;

import br.com.springbatchtest.demo.model.User;
import br.com.springbatchtest.demo.repository.UserRepository;
import lombok.Getter;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

@Getter
@Component
@JobScope
public class UserItemReader extends RepositoryItemReader<User> {

  @Autowired
  private UserRepository userRepository;

  @PostConstruct
  protected void init() {
    this.setRepository(this.getUserRepository());
    this.setMethodName("findAll"); // You should specify the method which
               //spring batch should call in your repository to fetch
               // data and the arguments it needs needs to be
               //specified with the below method.
               // And this method  must return Page<T>
//    this.setArguments(arguments);

    final Map<String, Sort.Direction> sorts = new HashMap<>();
    sorts.put("id", Sort.Direction.ASC);// This could be any field name of your Entity class
    this.setSort(sorts);

    this.setPageSize(20);
  }
}