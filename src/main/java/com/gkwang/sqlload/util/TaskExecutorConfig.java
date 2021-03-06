package com.gkwang.sqlload.util;

import java.util.concurrent.Executor;

import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;


public class TaskExecutorConfig implements AsyncConfigurer {
	@Override
    public Executor getAsyncExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        //核心线程数
        taskExecutor.setCorePoolSize(5);
        //设置池最大容量
        taskExecutor.setMaxPoolSize(10);
        //设置队列容量
        //如果池中线程已全部被使用，则将后续的任务添加到队列中等待执行
        //如果队列也满了，则新建线程
        //如果线程数已经达到池容量最大值
        //则抛出异常
        taskExecutor.setQueueCapacity(25);

        //初始化
        taskExecutor.initialize();

        return taskExecutor;
    }

    @Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {

        return null;
    }
}
