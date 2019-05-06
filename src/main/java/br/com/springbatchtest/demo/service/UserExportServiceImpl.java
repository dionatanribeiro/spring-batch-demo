package br.com.springbatchtest.demo.service;

import br.com.springbatchtest.demo.model.User;
import br.com.springbatchtest.demo.model.UserDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
public class UserExportServiceImpl implements UserExportService {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job job;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Async
    @Override
    public void exportUserToCsv() {
        Map<String, JobParameter> maps = new HashMap<>();
        maps.put("time", new JobParameter(System.currentTimeMillis()));
        JobParameters parameters = new JobParameters(maps);

        jobBuilderFactory.get("CSV-EXPORT-USERS")
                .incrementer(new RunIdIncrementer())
                .start(stepBuilderFactory.get("CSV-IMPORT-Users")
                        .<User, User>chunk(chunkSize)
                        .reader(itemReader)
                        .processor(itemProcessor)
                        .writer(dbWriter)
                        .build())
                .next(stepBuilderFactory.get("CSV-EXPORT-Users")
                        .<User, UserDto>chunk(chunkSize)
                        .reader(userDbItemReader)
                        .processor(exportProcessor)
                        .writer(csvWriter())
                        .build())
                .build();

        try {
            JobExecution jobExecution = jobLauncher.run(job, parameters);

            while (jobExecution.isRunning()) {
                System.out.println("...");
            }

        } catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException | JobParametersInvalidException e) {
            log.error("Error when execute batch job, {}", e);
        }

    }

}
