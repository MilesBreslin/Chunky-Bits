# Chunky Bits
[![Compile](https://github.com/MilesBreslin/Chunky-Bits/actions/workflows/compile.yml/badge.svg)](https://github.com/MilesBreslin/Chunky-Bits/actions/workflows/compile.yml)

Chunky Bits is simple, unmanaged, distributed HTTP object store tool. It is _not_ S3 compatible. Chunky Bits is built to handle the common user's cluster, where the cluster may grow/shrink at anytime and will consist of cheap, unbalanced hardware.

This is not intended to be a self-contained storage solution. Chunky Bits is a tool that only makes sense in a ecosystem of other common applications and tools. It is up to you to design your own storage solution. See the recommended cluster for more information.

##### Data Loss Warning

This software is currently in an alpha state. This is an education experiment, not a production tool. I take no responsibility for how you use this tool and any data-loss that may incur from it. You have been warned. See [LICENSE](./LICENSE) file for more details.

### Getting Started

Chunky Bits can be compiled with `cargo` which provides a single executable capable of providing a usable command-line application and a HTTP gateway. To get familiar with the tool, try out the following commands. You will need a `cluster.yaml` and you can find and modify one from the examples directory, such as the [local](./example/local.yaml) cluster file.

```bash
chunky-bits put TESTFILE --cluster cluster.yaml
chunky-bits decode-file --file path/to/metadata/TESTFILE --destination TESTFILE.2
```

The `put` command will write your file into the cluster and create a metadata reference to it. If you are using the `path` metadata type, you will be able to view it as a file.

The `decode-file` command will read the file from the cluster. However, since the metadata files contain all the metadata required to reference them, you do not need to specify a cluster to decode a metadata file.

### Design

Given a file, Chunky Bits will split it into parts. A file part will consist of `d` data chunks and `p` parity chunks. A single part will be distributed at least `d+p` destinations. A metadata file will be returned containing at least the checksums of each chunk, the locations of each chunk, and the length of the file.

A destination is anywhere a file can be referenced, currently including only local paths and HTTP URLs. One destination should be one physical disk running some filesystem of your choice.

A metadata file is stored in the JSON or YAML format. A minimal example is shown below.

```yaml
length: 52428800
parts:
  - data:
      - sha256: 4d589118cd5b236df24f79f951df8c4907098b19e25f45ffea3882d6ddcc2f37
        locations:
          - /mnt/repo4/4d589118cd5b236df24f79f951df8c4907098b19e25f45ffea3882d6ddcc2f37
      - ...
    parity:
      - sha256: 1b9acb5b2436dfa1cff8bb0ad39b317c14c8d07214a5a437275d617352ded59b
        locations:
          - https://node2.chunky-bits.local/1b9acb5b2436dfa1cff8bb0ad39b317c14c8d07214a5a437275d617352ded59b
      - ...
  - ...
```

In order to manually read a file without this tool, concatenate all of the data chunks together and limit the final length to the length specified.

```bash
for location in "${locations[@]}" ; do
    cat "$location"
done | head -c "$length"
```

The parity bits are byte-encoded reed solomon erasure coding. To repair a part, keep in mind that the both the data and parity parts are listed in-order and have no encoding on-top of them.

##### Why not par2?

While `par2` does simplify the repair process, the file format seems to lack a variety of implementation. I was unable to find one available for rust. To reduce the maintenance burden on Chunky Bits, plain reed solomon erasure coding was chosen instead, with no identifying file format.

A consequence of this is that the order of the chunks is mandatory. If someone else develops and maintains a rust `par2` implementation, I will happily consider adding `par2` support.

### Example Destination Configurations

A simple, monolithic object store could be designed using a single server with `n` disks. Each disk would have its own filesystem and would act as its own destination. This would provide `n` destinations. That same server could then provide its own HTTP gateway as well. Potential issues for this configuration are that it does not scale well since you need to get a larger server to increase either throughput or capacity and you will need to reboot for security patching. Other options to consider would be a RAID solution (hardware RAID card, Linux MD, ZFS, BTRFS, etc.) or a more enterprise-grade object storage solution like Minio, Ceph, and GlusterFS.

A simple, distributed object store could be designed using `n` servers with single disks each. Another server could then host its own HTTP gateway, pointing to each other server as a single destination. This would provide `n` destinations for `n+1` servers. Potential issues for this configuration are that traffic is duplicated since it must go from each destination to the gateway, the gateway is now a single point of failure, the gateway is also a single point to scale. Other options to consider are more enterprise-grade object storage solution like Minio, Ceph, and GlusterFS.

To expand on the distributed object store, you could make each server in the cluster its own HTTP gateway. This would allow for load-balancing and better failure handling. You could use some combination of DNS round-robin A-records, floating IPs, and dedicated load-balancers to make this scale very well. Again, enterprise-grade solutions can also do this as well.

A example to build upon the strengths of Chunky Bits would be a combination of all 3 of those use-cases above: 1 big server providing `n` destinations, `m` small servers providing 1 destination each, and some distributed gateways. In this configuration, the primary bottleneck is likely the 1 big server. For your writes, you should configure `d+p` chunks to be less than `n+m` destinations. This allows you to manually weight where your writes end up on a per-file basis. For more throughput on some files, weight them away from the 1 big server. To continue using the capacity of the 1 big server, weight some less-used file writes towards that server. This still has the problem of if that 1 big server goes down, you will likely loose access to some files. You may also weight destinations to 0, which will effectively remove them from the cluster for that write.

Going one-step further on the the combination example, you could purchase (or reuse) drives of mixed capacity. As long as your `d+p` is less than your `n` destinations, you can configure weights based on free disk space. 1 `16TiB` drive does not equal 2 `8TiB` drives entirely, since one large drive is a single point of failure and 2 smaller drives are 2 points of failure. However, are only writing `3` data chunks and `2` parity chunks and have `4` `8TiB` drive and `4` `16TiB` drives, you could weight your writes so the `16TiB` drives a about twice as likely to receive a chunk as a `8TiB` drive. While you would still only be able to fail `2` out of `8` drives in that situation, you would still be able to take advantage of the fill drive capacity of all of them.

All of the above solutions are under the assumption that all of your data will reside at a location that you manage servers at. A consequence of that is that you must buy up-front each individual drive, including the extra drives for parity. However, since a destination is just a WebDav end-point, that includes the ability to run S3-compatible object stores as endpoints. You could add a cloud-storage provider as `m` remote destinations in addition to your `n` on-site destinations. This would allow you to be able to fail `m` locally managed disks before data-loss. You would also be paying as you go for the remote storage and could weight your reads to only be from the local part of your cluster while it is operating normally, reducing cloud data egress costs.

Finally, all of the above solutions expect you to plan out your cluster ahead of time, which still is recommended. However, Chunky Bits will allow you to rebalance a file on a per-file basis, expand/shrink the cluster manually, use mismatched drives, and mix remote and local storage. Enterprise-grade tools typically expect you to have planned out your storage expansion in whole units, but given Chunky Bits' more free-form data distribution methods, that is not necessary.