# Neutral installed-extension package objects

This lane-owned fixture describes inert bytes used by
`test_extension_plan_seal.py`. The test builds deterministic wheel bytes in its
temporary directory, wraps them in the existing verified distribution-package
contract, and invokes the v2 planner without importing the fixture package.
The extension wheel contains an initializer, two sibling modules, and a shared
helper so the installed-package shape cannot collapse back to one source file.

`builder-program.json` is inert identity metadata retained for fixture-history
comparison. The executable builder authority is the exact source returned by
`extension_runtime_seal.builder_program_bytes()` and is compiled and digest-
checked by the focused test; declarative metadata is never treated as an
installer.

The fixture deliberately contains no checkout path, consumer name, executable
class path, package index, cache locator, endpoint, credential, or database
authority. Broker, SDK proxy, file-capability, invocation, and publication
fixtures live in their separately owned directories.
