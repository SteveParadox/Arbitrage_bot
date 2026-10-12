# Executed review evidence

Tested source: `87b2f6634e0419da4c40a6d724aa1549969f4b29`.
Subsequent delivery changes contain only this evidence and documentation.

- [Summary and exact counts](summary.json)
- [Full real-service offline run](full-offline.log): 375 Python tests; Redis reliability; migration workflow.
- [Rust tests](rust-tests.log), [Clippy](clippy.log), [build](build.log), [formatting](format.log).
- [Frontend tests](frontend-tests.log), [lint](frontend-lint.log), [build](frontend-build.log), [installation](npm-ci.log).
- [Python Ruff](python-ruff.log).
- [Profitable trace](profitable-cycle.json), [cash-closed losing recovery](losing-recovered-cycle.json), [partial-fill recovery](partial-recovered-cycle.json).
- [Old overspend reproduced](price-budget-reproduction.log), [zero-fill mismatch reproduced](zero-fill-reproduction.log), [corrected pipeline](price-budget-fixed.log), [contradictory price evidence](price-evidence.log).
- [Old liveness bugs reproduced](liveness-reproduction.log), [corrected liveness](liveness-fixed.log).
- [Paper short soak](paper-soak.json), [soak command log](paper-soak.log): 60.128 seconds, 50 closed cycles, independently balanced, immediate offline status after graceful exit.
- [Docker environment blocker](docker-blocker.log).
- [Changed files versus inspected main](changed-files.txt).

All HTTP exchange evidence is from a loopback-only mock using dummy credentials.
Financial trace amounts are synthetic. No private Bybit endpoint or actual
exchange order was used. The test-only UID shim used to launch stock PostgreSQL
in this restricted root container does not establish production security.

Other folders under `docs/evidence` retain prior paper-audit history; this folder
and its summary are the evidence for the authenticated review source above.

Focused cases are subsets of the full suite; do not add them to its total.
The 3 ignored Redis tests were subsequently executed against real Redis.
One Starlette deprecation warning remains; it is not an assertion failure.
