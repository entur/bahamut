package org.entur.bahamut.camel;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class ExecutionTimeLogger {
    private static final Logger logger = LoggerFactory.getLogger(ExecutionTimeLogger.class);

    @Around("execution(* *(..)) && @annotation(org.entur.bahamut.camel.LogExecutionTime)")
    public Object log(ProceedingJoinPoint point) throws Throwable {
        long start = System.currentTimeMillis();
        Object result = point.proceed();
        logger.info("className={}, methodName={}, timeMs={},threadId={}",
                point.getSignature().getDeclaringTypeName(),
                ((MethodSignature) point.getSignature()).getMethod().getName(),
                System.currentTimeMillis() - start,
                Thread.currentThread().getId());
        return result;
    }
}