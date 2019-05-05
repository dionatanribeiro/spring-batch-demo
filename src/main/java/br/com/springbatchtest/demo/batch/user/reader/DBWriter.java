package br.com.springbatchtest.demo.batch.user.reader;

import br.com.springbatchtest.demo.model.User;
import br.com.springbatchtest.demo.repository.UserRepository;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class DBWriter implements ItemWriter<User> {

    @Autowired
    private UserRepository userRepository;

    @Override
    public void write(List<? extends User> users) throws Exception {
        System.out.println("Data Saved for Users: " + users);
        userRepository.saveAll(users);
    }
}