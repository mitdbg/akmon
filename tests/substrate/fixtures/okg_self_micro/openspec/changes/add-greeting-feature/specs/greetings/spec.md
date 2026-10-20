# greetings Delta

## ADDED Requirements

### Requirement: Hello Function

The system SHALL provide a `hello(name)` function returning a
greeting string for the named subject.

#### Scenario: hello returns greeting

- **WHEN** `hello("world")` is called
- **THEN** the result SHALL be `"hello, world"`
