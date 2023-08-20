package io.spring.springbatch;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.*;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@RequiredArgsConstructor
@Configuration
public class ParallelStepConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;

    @Bean
    public Job job() throws Exception {
        return jobBuilderFactory.get("batchJob")
                .incrementer(new RunIdIncrementer())
                .start(flow1())
//                .next(flow2())
                .split(taskExecutor()).add(flow2())
                .end()
                .listener(new StopWatchJobListener())
                .build();
    }

    @Bean
    public Flow flow1() {

        TaskletStep step1 = stepBuilderFactory.get("step1")
                .tasklet(tasklet())
                .build();

        return new FlowBuilder<Flow>("flow1")
                .start(step1)
                .build();
    }
    @Bean
    public Flow flow2() {

        TaskletStep step2 = stepBuilderFactory.get("step2")
                .tasklet(tasklet())
                .build();

        TaskletStep step3 = stepBuilderFactory.get("step3")
                .tasklet(tasklet())
                .build();


        return new FlowBuilder<Flow>("flow2")
                .start(step2)
                .next(step3)
                .build();
    }

    @Bean
    public Tasklet tasklet() {
        return new CustomTasklet();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(2); // 4개의 스레드로 작업
        taskExecutor.setMaxPoolSize(4); // 아직 남은 처리가 있을 떄 생성할 최대 스레드 개수
        taskExecutor.setThreadNamePrefix("async-thread-");

        return taskExecutor;
    }


}