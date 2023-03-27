# spring-retry-bug

Run the tests to see the bug.

`ExpectedBehaviourTest` throws a FatalException and correctly writes the record to the DLT.

`ReproduceBugTest` throws at first a RetryException so the record gets -- retried. In this second attempt of processing the record a `FatalException` is thrown, that should put the record to the DLT, but is retried instead.
