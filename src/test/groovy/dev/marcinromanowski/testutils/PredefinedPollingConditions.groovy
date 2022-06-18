package dev.marcinromanowski.testutils

import groovy.transform.CompileStatic
import org.spockframework.runtime.GroovyRuntimeUtil
import org.spockframework.runtime.SpockTimeoutError
import spock.util.concurrent.PollingConditions

import java.time.Duration

/**
 * This class gives you default PoolingConditions, that can be customized by system properties, so that
 * for example on your local machine an assertion fails after 1 sec of waiting, but on integration test in Azure
 * it fails after 10 sec (shared CI workers tend to be slow).
 *
 * Use like this:
 * <pre>
 *    WAIT.eventually {
 *          assert exampleKafkaListener.getReceivedValues(KEY) == [DATA]
 *    }
 * </pre>
 *
 * And if you want to wait longer/shorter on a given environment set System property of tests.polling.timeout.multiplier
 *
 * Kudos to Bartek Wojtkiewicz for the idea
 */
@CompileStatic
class PredefinedPollingConditions {

    public static final PollingConditions SHORT_WAIT = new PollingConditions(timeout: calculateTimeoutIncludingEnvironment(timeout(DEFAULT_SHORT)))
    public static final PollingConditions WAIT = new PollingConditions(timeout: calculateTimeoutIncludingEnvironment(timeout(DEFAULT_MEDIUM)))
    public static final PollingConditions LONG_WAIT = new PollingConditions(timeout: calculateTimeoutIncludingEnvironment(timeout(DEFAULT_LONG)))
    public static final PollingConditions SHORT_WAIT_WITH_INITIAL_DELAY = new PollingConditions(
            timeout: calculateTimeoutIncludingEnvironment(timeout(DEFAULT_SHORT)) + 1,
            initialDelay: calculateTimeoutIncludingEnvironment(timeout(DEFAULT_SHORT)))

    private static final String MULTIPLIER_PROPERTY = "tests.polling.timeout.multiplier"
    private static final int DEFAULT_SHORT = 1
    private static final int DEFAULT_MEDIUM = 10
    private static final int DEFAULT_LONG = 30

    static void constantly(long timeInMillisToMaintainCondition, Closure conditions) {
        long lastAttempt
        long start = System.currentTimeMillis()
        long elapsedTime = 0

        while (elapsedTime < timeInMillisToMaintainCondition) {
            try {
                GroovyRuntimeUtil.invokeClosure(conditions)
                lastAttempt = System.currentTimeMillis()
            } catch (Throwable e) {
                String msg = String.format("Condition not maintained within %1.2f seconds", timeInMillisToMaintainCondition / 1000d)
                throw new SpockTimeoutError(timeInMillisToMaintainCondition, msg, e)
            }

            elapsedTime = lastAttempt - start
        }
    }

    private static <T extends Number> T calculateTimeoutIncludingEnvironment(T timeout) {
        def result = System.getenv().get(MULTIPLIER_PROPERTY) == null
                ? timeout
                : Integer.valueOf(System.getenv().get(MULTIPLIER_PROPERTY)) * timeout as T

        println("calculateTimeoutIncludingEnvironment: $timeout -> $result")
        return result
    }

    private static int timeout(int defaultSeconds) {
        return System.getProperty(MULTIPLIER_PROPERTY) == null
                ? defaultSeconds
                : Integer.valueOf(System.getProperty(MULTIPLIER_PROPERTY)) * defaultSeconds
    }

    static class DurationW {
        public static final Duration DEFAULT_D = Duration.ofSeconds(DEFAULT_MEDIUM)
        public static final Duration SHORT_D = Duration.ofSeconds(DEFAULT_SHORT)
        public static final Duration LONG_D = Duration.ofSeconds(DEFAULT_LONG)
    }

}
