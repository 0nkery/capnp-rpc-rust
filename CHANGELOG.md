### v0.7.4
- Eliminate some calls to unwrap(), in favor of saner error handling.
- Eliminate dependency on capnp/c++.capnp.

### v0.7.3
- Directly include rpc.capnp and rpc-twoparty.capnp to make the build more robust.

### v0.7.2
- Fix "unimplemented" panic that could happen on certain broken capabilities.

### v0.7.1
- Fix bug where piplining on a method that returned a null capability could cause a panic.
