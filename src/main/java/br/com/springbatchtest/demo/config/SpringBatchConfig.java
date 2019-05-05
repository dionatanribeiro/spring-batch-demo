package br.com.springbatchtest.demo.config;

import br.com.springbatchtest.demo.batch.user.reader.DBWriter;
import br.com.springbatchtest.demo.model.User;
import br.com.springbatchtest.demo.model.UserSpecification;
import br.com.springbatchtest.demo.repository.UserRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {

    @Value("${batch.chunk.size}")
    private int chunkSize;

    private static final String CSV_DELIMITER = ";";

    private Resource outputResource = new FileSystemResource("/home/dionatan/workspace/spring-batch-test/src/main/resources/output/outputData.csv");

    @Autowired
    private DBWriter dbWriter;

    @Bean
    public Job job(JobBuilderFactory jobBuilderFactory,
                                   StepBuilderFactory stepBuilderFactory,
                                   @Qualifier("csvReader") ItemReader<User> itemReader,
                                   ItemProcessor<User, User> itemProcessor,
                                   @Qualifier("userDbItemReader") ItemReader<User> userDbItemReader
    ) {

        Step step1 = stepBuilderFactory.get("CSV-IMPORT-Users")
                .<User, User>chunk(chunkSize)
                .reader(itemReader)
                .processor(itemProcessor)
                .writer(dbWriter)
                .build();

        Step step2 = stepBuilderFactory.get("CSV-EXPORT-Users")
                .<User, User>chunk(chunkSize)
                .reader(userDbItemReader)
                .writer(csvWriter())
                .build();

//        return jobBuilderFactory.get("CSV-IMPORT")
//                .incrementer(new RunIdIncrementer())
//                    .flow(step1)
//                    .next(step2)
//                    .end()
//                .build();

        return jobBuilderFactory.get("CSV-FLOW")
                .incrementer(new RunIdIncrementer())
                .start(step1)
                .next(step2)
                .build();
    }

    @Bean("csvReader")
    public FlatFileItemReader<User> itemReader(@Value("${input}") Resource resource) {
        FlatFileItemReader<User> flatFileItemReader = new FlatFileItemReader<>();
        flatFileItemReader.setResource(resource);
        flatFileItemReader.setName("CSV-Reader");
        flatFileItemReader.setLinesToSkip(1);
        flatFileItemReader.setLineMapper(lineMapper());
        return flatFileItemReader;
    }

    @Bean
    public LineMapper<User> lineMapper() {

        DefaultLineMapper<User> defaultLineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();

        lineTokenizer.setDelimiter(CSV_DELIMITER);
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("name", "dept", "salary");

        BeanWrapperFieldSetMapper<User> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(User.class);

        defaultLineMapper.setLineTokenizer(lineTokenizer);
        defaultLineMapper.setFieldSetMapper(fieldSetMapper);

        return defaultLineMapper;
    }

    public ItemWriter<User> csvWriter()
    {
        //Create writer instance
        FlatFileItemWriter<User> writer = new FlatFileItemWriter<>();

        //Set output file location
        writer.setResource(outputResource);

        //All job repetitions should "append" to same output file
        writer.setAppendAllowed(true);

        writer.setEncoding("UTF-8");

        String[] headers = { "id", "name", "dept", "salary", "time" };

        // write first line by headers string
        writer.setHeaderCallback(w -> w.write(Stream.of(headers)
                .collect(Collectors.joining(CSV_DELIMITER))));

        //Name field values sequence based on object properties
        writer.setLineAggregator(new DelimitedLineAggregator<User>() {
            {
                setDelimiter(CSV_DELIMITER);
                setFieldExtractor(new BeanWrapperFieldExtractor<User>() {
                    {
                        setNames(headers);
                    }
                });
            }
        });
        return writer;
    }

    @Bean
    public ItemReader<User> userDbItemReader(@Autowired UserRepository userRepository) {
        final RepositoryItemReader<User> repositoryItemReader = new RepositoryItemReader<>();
        repositoryItemReader.setRepository(userRepository);
        repositoryItemReader.setMethodName("findAll");
        repositoryItemReader.setPageSize(20);

        // method arguments
        List<Specification> arguments = new ArrayList<>();
        arguments.add(UserSpecification.dept("Operations"));
        repositoryItemReader.setArguments(arguments);

        final Map<String, Sort.Direction> sorts = new HashMap<>();
        sorts.put("id", Sort.Direction.ASC);
        repositoryItemReader.setSort(sorts);

        return repositoryItemReader;
    }

}