# threema-cli-rs

This is supposed to be a Threema Desktop client. It aims to be lightweight, implemented in pure rust with minimal dependencies (no Electron involved). 

Note: Due to [license restrictions](https://web.archive.org/web/20260608030221/https://threema.com/en/why-threema/open-source), it is not allowed to create a Threema ID on a custom build:

> If you would like to use a self-compiled app, please restore the backup of an existing Threema ID. You can create Threema IDs and backups thereof using the purchased app.

Consequently, this client is supposed to be linked against a primary device running the official client – just like Threema Desktop.

## State

Currently, it does not really work. A connection can be established, messages are received but encryption fails.

A [patched version of libthreema](https://github.com/hoehermann/threema-desktop/tree/stable/packages/libthreema-wasm/libs/libthreema) is used as the official version does not expose all functions in the way an external crate needs them.

## Obtaining Secrets

Since at least a part of the linking procedure is implemented in TypeScript and I do not want Electron (or any ECMAScript runtime) as a dependency, the multi-device secrets must be extracted from a currently linked Threema Desktop instance.

A `printMultiDeviceSecrets` command was added to the official nodejs-based CLI in [this fork](https://github.com/hoehermann/threema-desktop/tree/feature/print-secrets).

```
pnpm run dev:desktop:consumer-live
pnpm build:desktop:cli
node ./apps/desktop/build/cli/cli/bin.cjs printMultiDeviceSecrets ~/.local/share/ThreemaDesktop/consumer-live-default
```

Of course this means that particular Threema Desktop instance and threema-cli-rs must not be used at the same time, but at least the primary device can be used in parallel.

## Alternatives

Similar projects exist, but have not been updated in years and probably do not implement the multi-device features this project is geared towards.

* https://github.com/thejonny/threema-client-rs
* https://github.com/o3ma/o3
