package br.com.springbatchtest.demo.controller;

import br.com.springbatchtest.demo.model.User;
import br.com.springbatchtest.demo.model.UserSpecification;
import br.com.springbatchtest.demo.repository.UserRepository;
import br.com.springbatchtest.demo.service.UserExportService;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/load")
public class LoadController {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job job;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private UserExportService userExportService;

    @GetMapping("/teste")
    public Page<User> teste() {
        Specification<User> operations = UserSpecification.dept("Operations");
        PageRequest pageRequest = PageRequest.of(0, 20);
        return userRepository.findAll(operations, pageRequest);
    }

    @GetMapping
    public BatchStatus load() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {

        Map<String, JobParameter> maps = new HashMap<>();
        maps.put("time", new JobParameter(System.currentTimeMillis()));
        JobParameters parameters = new JobParameters(maps);
        JobExecution jobExecution = jobLauncher.run(job, parameters);

        System.out.println("JobExecution: " + jobExecution.getStatus());

        System.out.println("Batch is Running...");

        while (jobExecution.isRunning()) {
//            jobExecution.stop();
            System.out.println("...");
        }

        return jobExecution.getStatus();
    }

    @GetMapping("/async")
    public ResponseEntity<String> loadAsync() {
        userExportService.exportUserToCsv();
        return ResponseEntity.status(HttpStatus.ACCEPTED).body("Job iniciado");
    }

}