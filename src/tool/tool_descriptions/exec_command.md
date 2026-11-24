Start a shell command inside the workspace. Valid startup input uses `cmd` plus optional `description`, `workdir`, `shell`, `login`, `tty`, `duplicate_policy`, `accepts_input`, `yield_time_ms`, and `max_output_tokens`; do not pass result or task metadata such as `status` or `task_handle`. `duplicate_policy` defaults to `reuse_running` and `yield_time_ms` defaults to 10_000 ms when omitted; set it only when intentionally changing the foreground wait window. Short commands return immediately; long non-interactive commands become command_task automatically.

> ⚠ ripgrep note: `-r` means `--replace`, not recursive. Recursion is the default. `rg -rn "x"` is parsed as `--replace n`; use `rg -n "x"` instead.

`description` is an optional short caption naming the concrete target and action of this invocation. It is persisted with the tool input for display; it is never evaluated as shell content and does not replace `cmd`. Do not claim success or completion before the command returns.
