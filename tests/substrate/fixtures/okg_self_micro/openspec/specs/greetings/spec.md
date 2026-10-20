# greetings Specification

## Purpose

Greeting helpers for the okg_self_micro fixture corpus.

## Requirements

### Requirement: Hello Function

The system SHALL provide a `hello(name, *, formal=False)`
function returning a greeting string.

#### Scenario: casual mode

- **WHEN** `hello("world")` is called
- **THEN** the result SHALL be `"hello, world"`

#### Scenario: formal mode

- **WHEN** `hello("world", formal=True)` is called
- **THEN** the result SHALL be `"Greetings, world."`
