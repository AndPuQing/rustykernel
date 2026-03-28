# ipykernel Compatibility Matrix

This document classifies the current `ipykernel` comparison coverage into the
three buckets used by the roadmap:

- Aligned (`已对齐`)
- Partial (`部分对齐`)
- Not covered (`未覆盖`)

The source test file is `tests/test_ipykernel_compat.py`.

## Aligned

These checks compare the current `rustykernel` behavior against `ipykernel`
without leaving meaningful reply fields unexamined for the tested scenario.

| Surface | Evidence | Why it is classified as aligned |
| --- | --- | --- |
| `comm_info_request` in the empty-comm case | `test_comm_info_reply_matches_ipykernel` | The test compares the full reply payload directly. Today both kernels return `{"status": "ok", "comms": {}}` for this scenario. |
| `history_request` range query after two simple executions | `test_history_reply_matches_ipykernel_for_range_query` | The test compares the full reply payload directly for a concrete range-history scenario, including stored outputs. |
| `is_complete_request` for complete / incomplete / invalid syntax | `test_is_complete_reply_matches_ipykernel` | The test compares the semantic reply fields that matter for this message type: `status` and `indent`, across three canonical inputs. |
| `shutdown_request` with `restart: false` | `test_shutdown_request_matches_ipykernel` | The test compares the full control reply and normalized IOPub sequence for the non-restart shutdown path, including the extra `shutdown_reply` published on IOPub. |

## Partial

These checks show that core semantics are close to `ipykernel`, but they only
compare a normalized subset or a narrow happy-path scenario.

| Surface | Evidence | Why it is only partial |
| --- | --- | --- |
| `kernel_info_request` | `test_kernel_info_reply_matches_ipykernel` | The test normalizes the reply before comparison. It ignores fields such as `banner`, `implementation`, `implementation_version`, and the full `help_links` list. It also still ignores raw `debugger` / `supported_features` differences because `ipykernel` advertises `supported_features` differently and does not expose the same `implementation` surface. |
| `connect_request` | `test_connect_reply_matches_ipykernel_semantics` | The test compares whether each kernel correctly echoes the ports from its own connection payload. It validates the reply semantics, but not raw cross-kernel equality because the bound ports differ by instance. |
| `execute_request` success path | `test_execute_result_flow_matches_ipykernel` | The test covers one successful expression-result flow and normalizes the reply and IOPub sequence. It does not compare stream output, silent execution, rich display updates, or stdin-driven execution. |
| `execute_request` runtime error path | `test_execute_error_flow_matches_ipykernel` | The test compares one runtime error flow after normalizing away `ipykernel`'s extra `engine_info`. It covers `ename`, `evalue`, `traceback`, `user_expressions`, `payload`, and the `error` IOPub message, but not broader exception classes or stream side effects around failures. |
| `execute_request` syntax error path | `test_execute_syntax_error_flow_matches_ipykernel_semantics` | The test compares one syntax-error flow semantically. It normalizes filename-sensitive `evalue` text and compares the traceback tail rather than requiring exact raw internal-frame formatting. |
| `interrupt_request` plus interrupted execution | `test_interrupt_request_matches_ipykernel_semantics` | The test compares the control-channel `interrupt_reply`, its `busy`/`idle` IOPub status pair, and the resulting `KeyboardInterrupt` execute error flow. It still covers only one long-running sleep-based scenario. |
| `complete_request` | `test_complete_reply_matches_ipykernel_after_normalization` | The test compares normalized matches and normalized completion metadata. It does not require exact raw ordering or exact metadata parity beyond the normalized type/signature shape. |
| `inspect_request` | `test_inspect_reply_matches_ipykernel_core_semantics` | The test compares extracted semantics from `text/plain`, not the full raw payload. Markdown, HTML, metadata details, and formatting differences are intentionally ignored. |

## Not Covered

These parts of the current protocol surface are implemented in `rustykernel`,
but are not compared against `ipykernel` by the existing compatibility suite.

| Surface | Current state |
| --- | --- |
| stdin flow: `input_request` / `input_reply` | Implemented locally, but no `ipykernel` comparison test exists. |
| `usage_request` | Implemented locally on the control channel and covered by smoke/runtime tests, but no `ipykernel` comparison test exists yet because the payload is host- and process-sensitive. |
| `execute_request` stream behavior and broader failure cases | Runtime errors, one syntax-error scenario, and one interrupt-induced `KeyboardInterrupt` scenario are compared against `ipykernel`, but stream behavior, other exception classes, and broader interrupt edge cases are not. |
| `execute_request` rich display behavior | Covered by local tests for `display_data` / `update_display_data`, but not compared against `ipykernel`. |
| `execute_request` with `user_expressions` | Covered by local tests, but not compared against `ipykernel`. |

## Practical Readout

At the moment, the comparison suite shows:

- exact alignment for a narrow subset of request types
- semantic alignment for several high-value requests through normalized checks
- no direct `ipykernel` comparison yet for some implemented control and history
  surfaces

That means the project can already say "which requests are being compared to
`ipykernel`" and "how strong each comparison currently is", but it cannot yet
claim broad one-to-one parity across the full implemented protocol surface.
