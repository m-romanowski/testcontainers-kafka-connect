package dev.marcinromanowski.testutils

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import java.util.concurrent.ConcurrentHashMap

@Slf4j
@CompileStatic
final class MockConsumers {

    private static final Map<String, Set<MockConsumer>> consumersMap = new ConcurrentHashMap<>()

    static Collection<MockConsumer> getMockConsumers(String name) {
        return Collections.unmodifiableCollection(consumersMap.getOrDefault(name, Collections.EMPTY_SET))
    }

    static MockConsumer registerMockConsumer(String name) {
        def consumedValues = []
        MockConsumer mockConsumer = Mockito.mock(MockConsumer.class)
        Mockito.when(mockConsumer.getName()).thenReturn(name)
        Mockito.when(mockConsumer.consumed(Mockito.any())).thenAnswer({
            consumedValues.addAll(it.getArguments())
            return new Answer<Object>() {
                @Override
                Object answer(InvocationOnMock invocation) throws Throwable {
                    return it.arguments
                }
            }
        })
        Mockito.when(mockConsumer.consumed(Mockito.any(), Mockito.any())).thenAnswer({
            consumedValues.add(it.getArgument(1))
            return new Answer<Object>() {
                @Override
                Object answer(InvocationOnMock invocation) throws Throwable {
                    return it.arguments
                }
            }
        })
        Mockito.when(mockConsumer.getAllValues()).thenReturn(consumedValues)
        Mockito.when(mockConsumer.getValue()).thenAnswer(
                new Answer<Object>() {
                    @Override
                    Object answer(InvocationOnMock invocation) throws Throwable {
                        consumedValues.isEmpty() ? Optional.empty() : Optional.of(consumedValues.last())
                    }
                }
        )

        consumersMap.computeIfAbsent(name, { new HashSet<>() }).add(mockConsumer)
        log.info("Created mock consumer for name: {}", name)
        return mockConsumer
    }

    static void clearInvocationsOnMockConsumers() {
        consumersMap.each {
            Mockito.clearInvocations(it.getValue().toArray())
        }
    }

    static void clearInvocationsOnMockConsumers(MockConsumer... mockConsumer) {
        mockConsumer.each {
            Mockito.clearInvocations(it)
            log.info("Cleared invocations for mock consumer: {}", it.getName())
        }
    }

    static void unregisterMockConsumers() {
        consumersMap.clear()
        log.info("Removed all mock consumers")
    }

    static void unregisterMockConsumers(MockConsumer... mockConsumers) {
        mockConsumers.each {
            unregisterMockConsumer(it)
        }
    }

    static void unregisterMockConsumer(MockConsumer mockConsumer) {
        consumersMap.getOrDefault(mockConsumer.name, Collections.EMPTY_SET).remove(mockConsumer)
        log.info("Unregistered mock consumer: {}", mockConsumer.getName())
    }

}
