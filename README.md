# SDAG

### Building the source

```
git clone https://github.com/smart-dag/sdag.git
cd sdag
cargo build
```

See [Building the source](https://github.com/smart-dag/docs/blob/master/build/README.md) for more information

### Docker quick start

```
docker pull registry.cn-beijing.aliyuncs.com/sdag/sdag_testnet_dev:latest
docker run --rm -d --name sdag -p 6615:6615 -p 8080:8080 registry.cn-beijing.aliyuncs.com/sdag/sdag_testnet_dev
```

See [Docker quick start](https://github.com/smart-dag/docs/blob/master/start-docker/README.md) for more information

### License

SDAG is released under the terms of the LGPL-3.0 license. See [COPYING](COPYING) for more information or see https://opensource.org/licenses/LGPL-3.0
