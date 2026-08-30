# threema-multidevice-client

This library aims to be lightweight Threema Desktop 2.0 multi-device client library, implemented in pure rust with minimal dependencies (no Electron involved).

Note: Due to [license restrictions](https://web.archive.org/web/20260608030221/https://threema.com/en/why-threema/open-source), it is not allowed to create a Threema ID on a custom build:

> If you would like to use a self-compiled app, please restore the backup of an existing Threema ID. You can create Threema IDs and backups thereof using the purchased app.

Consequently, this client is supposed to be linked against a primary device running the official app.

## State

Currently, reflected messages can be received. A CLI example exists for demonstration purposes.

A [patched version of libthreema](https://github.com/hoehermann/threema-desktop/tree/threema-multidevice-client/packages/libthreema-wasm/libs/libthreema) is used since the official version does not expose all functions in the way an external crate needs them. By default it is fetched from that branch on GitHub. For local development against an unpushed checkout, clone `threema-desktop` as a sibling directory and add a `.cargo/config.toml` (gitignored) with:

```toml
[patch."https://github.com/hoehermann/threema-desktop"]
libthreema = { path = "../threema-desktop/packages/libthreema-wasm/libs/libthreema/lib" }

[patch.crates-io]
blake2 = { path = "../threema-desktop/packages/libthreema-wasm/libs/libthreema/patches/blake2" }
```

## Obtaining Secrets

Since at least a part of the linking procedure is implemented in TypeScript and I do not want Electron (or any ECMAScript runtime) as a dependency, the multi-device secrets must be extracted from a currently linked Threema Desktop instance.

A `printMultiDeviceSecrets` command was added to the official nodejs-based CLI in [this branch](https://github.com/hoehermann/threema-desktop/tree/feature/print-secrets).

```
pnpm run dev:desktop:consumer-live  
pnpm build:desktop:cli  
node ./apps/desktop/build/cli/cli/bin.cjs printMultiDeviceSecrets ~/.local/share/ThreemaDesktop/consumer-live-default
```

Of course this means the particular Threema Desktop instance and any threema-multidevice-client application must not be used at the same time. The primary device can be used in parallel.

## Alternatives

Similar projects exist, but have not been updated in years and probably do not implement the multi-device features.

- [https://github.com/thejonny/threema-client-rs](https://github.com/thejonny/threema-client-rs)
- [https://github.com/o3ma/o3](https://github.com/o3ma/o3)

