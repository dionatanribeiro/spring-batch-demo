package br.com.springbatchtest.demo.batch.user.exporter;

import br.com.springbatchtest.demo.model.User;
import br.com.springbatchtest.demo.model.UserDto;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class ExportProcessor implements ItemProcessor<User, UserDto> {

    @Override
    public UserDto process(User user) throws Exception {
        return UserDto.builder()
                .id(user.getId())
                .name(user.getName())
                .salary(user.getSalary())
                .time(user.getTime())
                .build();
    }
}