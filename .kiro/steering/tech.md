# Technology Stack

## Build System
- **Gradle** with Kotlin DSL support
- Java 11+ target compatibility (compiled bytecode)
- Multi-module project structure with examples and integration tests

## Core Dependencies
- **AWS SDK for Java v2** - S3 client and transfer manager
- **AWS CRT (Common Runtime)** - High-performance HTTP client
- **RxJava 3** - Reactive programming support
- **Caffeine** - High-performance caching library
- **SLF4J** - Logging facade

## Testing Framework
- **JUnit 5** - Primary testing framework
- **AssertJ** - Fluent assertions
- **Mockito** - Mocking framework
- **jqwik** - Property-based testing
- **Testcontainers** - Integration testing with LocalStack
- **System Lambda** - System property testing utilities

## Code Quality Tools
- **Checkstyle** - Code style enforcement (Google Java Style based)
- **JaCoCo** - Code coverage analysis (80% minimum coverage)
- **SpotBugs** - Static analysis
- **Gradle Shadow Plugin** - Fat JAR creation

## Common Commands

### Building
```bash
./gradlew build                    # Full build with tests
./gradlew shadowJar               # Create fat JAR with dependencies
./gradlew publishToMavenLocal     # Install to local Maven repository
```

### Testing
```bash
./gradlew test                    # Run unit tests
./gradlew integrationTest         # Run integration tests (requires Docker)
./gradlew testFullCodeCoverageReport  # Generate combined coverage report
```

### Code Quality
```bash
./gradlew checkstyleMain          # Check main code style
./gradlew checkstyleTest          # Check test code style
./gradlew jacocoTestReport        # Generate coverage report
```

## Java Compatibility
- Library versions >= 2.x.x require Java 11+
- Library versions <= 1.2.1 support Java 8+
- Build process may require newer JDK for Gradle execution