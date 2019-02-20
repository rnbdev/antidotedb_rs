Rust client for [Antidote DB][1]

It uses [Protobuf][2] to communicate an Antidote node.

-   Registers - Last-writer-wins, Multi-value
-   Counter - With-reset, Without-reset
-   Flag - Enable-wins, Disable-wins
-   Map - Grow-only, Recursive-remove
-   Set - Add-wins, Remove-wins

#### Get started

Checkout the [`examples`](examples/).

[1]: https://www.antidotedb.eu

[2]: https://antidotedb.gitbook.io/documentation/api/protocol-buffer-api
