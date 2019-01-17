### 如何使用sdg


首先你必须安装了Rust，如果没有的话，需要一条命令进行安装：

```
$ curl -sSf https://static.rust-lang.org/rustup.sh | sh
```

接下来是克隆仓库


```
https://gitlab.com/smart-dag/sdag.git
```


然后进入sdag 目录，使用命令cargo build即可。


```
sdag$ cargo build
...
Finished dev [unoptimized + debuginfo] target(s) in 0.47s
```


接下来分别进入  witness 、 hub 和 sdg 的目录。

编译witness：
```
witness$ cargo build
Compiling may_signal v0.1.0 (https://github.com/Xudong-Huang/may_signal.git#65f8feb2)
Compiling sdag_witness v0.1.0 (/home/cr4fun/sdag/sdag/witness)               
Finished dev [unoptimized + debuginfo] target(s) in 9.52s   
```

编译hub：

```
hub$ cargo build 
Compiling sdag v0.2.0 (/home/cr4fun/sdag/sdag)                               
Compiling sdag_hub v0.1.0 (/home/cr4fun/sdag/sdag/hub)              
Finished dev [unoptimized + debuginfo] target(s) in 22.16s
```

编译sdg：

```
sdg$ cargo build
...
Finished dev [unoptimized + debuginfo] target(s) in 0.20s

```
之后，进入你就可以使用sdg了。


###　如何使用　sdg

> sdg　是命令行的钱包。


* 初始化

```
/sdg$ cargo run init
    Finished dev [unoptimized + debuginfo] target(s) in 0.17s                   
     Running `/home/cr4fun/sdag/sdag/target/debug/sdg init`
settings:
{
  "hub_url": [
    "127.0.0.1:6615"
  ],
  "mnemonic": "enemy federal fashion danger dove tragic detail orange wood debate barely brick"
}

```
* 查看余额

因为本地没有启动hub，所以我们用内网的hub（以后提供测试网的hub）


```
nano settings.json
```

把hub改为： 10.168.3.131:6635


再运行 cargo run info

```
sdg$ cargo run info
    Finished dev [unoptimized + debuginfo] target(s) in 0.18s                   
     Running `/home/cr4fun/sdag/sdag/target/debug/sdg info`

current wallet info:

device_address: 0TJLSRFX5CAS7HNS545NMN5YOJ2OACW5C
wallet_public_key: xpub6D6kq6EV7foUEbsPkTMqQWCfbwR39zvoctYdGDa84wQiFc5PrJ9GJ6Mj27vaMUh79fcJUsrFeCFY7tAdpz6mJ1Pdj5NxGnnt3GX1qVkeo1X
└──wallet_id(0): Er7UdVYDYh4bBPKKWpJoWvWd2jYhOSmWvY3feXcXVlo=
   └──address(0/0): LP3VGPUTQV3O76F7K3YCABCL6G7FIUBN
      ├── path: /m/44'/0'/0'/0/0
      ├── pubkey: A3gO3JmLg+bMMIDMTuE8gZFlz5dDBSHm6R6DPJG2VaN0
      └── balance: 0.000000
```

这样就看到了余额。也可以在后面加上 -j 以查看json格式：

```
sdg$ cargo run info -j
    Finished dev [unoptimized + debuginfo] target(s) in 0.17s                   
     Running `/home/cr4fun/sdag/sdag/target/debug/sdg info -j`
{
  "device_address": "0TJLSRFX5CAS7HNS545NMN5YOJ2OACW5C",
  "wallet_public_key": "xpub6D6kq6EV7foUEbsPkTMqQWCfbwR39zvoctYdGDa84wQiFc5PrJ9GJ6Mj27vaMUh79fcJUsrFeCFY7tAdpz6mJ1Pdj5NxGnnt3GX1qVkeo1X",
  "wallet_id": "Er7UdVYDYh4bBPKKWpJoWvWd2jYhOSmWvY3feXcXVlo=",
  "address": "LP3VGPUTQV3O76F7K3YCABCL6G7FIUBN",
  "path": "/m/44'/0'/0'/0/0",
  "pubkey": "A3gO3JmLg+bMMIDMTuE8gZFlz5dDBSHm6R6DPJG2VaN0",
  "balance": "0"
}

```

* 转账

> 7E5V7WKXWC4ZELYSRYSA3UO6K53SELYC 是对方地址 ； 
10 是转账金额，单位是dag ；
hello 是上链信息。

```
sdg$ cargo run send --pay 7E5V7WKXWC4ZELYSRYSA3UO6K53SELYC 10 --text hello
Finished dev [unoptimized + debuginfo] target(s) in 0.67s                                                             
Running `/home/cr4fun/sdag/sdag/target/debug/sdg send --pay 7E5V7WKXWC4ZELYSRYSA3UO6K53SELYC 10 --text hello`
FROM  : 5YJKYU5NFWEUJAO4M2WNR4O3W5Z62ZYO
TO    : 
      address : 7E5V7WKXWC4ZELYSRYSA3UO6K53SELYC, amount : 10
UNIT  : YiNYIEe11O/3FegyzP4Aixl+6HxsjscgDKVKnc8q7Ro=
TEXT  : hello
DATE  : 2019-01-17 18:10:12.277
```

* 查看网络信息

```
 cargo run net
    Finished dev [unoptimized + debuginfo] target(s) in 0.18s                   
     Running `/home/cr4fun/sdag/sdag/target/debug/sdg net`
{
  "in_bounds": [
    {
      "peer_id": "tYDkaQRSPsx4GGwE86x2qWr+pse/aLaPSPn7GqSh",
      "peer_addr": "172.17.0.2:57702",
      "is_source": true,
      "is_subscribed": true
    },
    {
      "peer_id": "s6Q0m/RjYJD8PAr8wCwQAe7zfHQdDkgbu2+7KhiI",
      "peer_addr": "172.17.0.2:57404",
      "is_source": true,
      "is_subscribed": true
    },
    {
      "peer_id": "xNz7cZRaxrmaQZpPOkpur2poFRW97t9j0s9TC1CA",
      "peer_addr": "172.17.0.2:57400",
      "is_source": true,
      "is_subscribed": true
    },
    {
      "peer_id": "w4ho7yW4nHUQsxV62osDFRiNuafvIeNOK5yU2i5f",
      "peer_addr": "172.17.0.2:57696",
      "is_source": true,
      "is_subscribed": true
    },
    {
      "peer_id": "Qi9Jc95An4er4oitZTpTmKTscKi0hXjVUzCWUPht",
      "peer_addr": "172.17.0.2:57402",
      "is_source": true,
      "is_subscribed": true
    },
    {
      "peer_id": "l02NkUJjQhFWd6kQST6Mo0Mf419AEtd3PmgfoaUJ",
      "peer_addr": "172.17.0.2:57426",
      "is_source": true,
      "is_subscribed": true
    },
    {
      "peer_id": "2ergjPN0BbiYPseZEleR95imzpw9exVyKsjAGtPd",
      "peer_addr": "172.17.0.2:57424",
      "is_source": true,
      "is_subscribed": true
    },
    {
      "peer_id": "TaPzS52Xbyscl4KTHeNLfZXkZmmrZwgQTQaoVRTW",
      "peer_addr": "10.168.1.111:47038",
      "is_source": false,
      "is_subscribed": false
    },
    {
      "peer_id": "H4wccpp53ua6sAQnrjUaXgsXp1Ys0Su5c01uXDmL",
      "peer_addr": "172.17.0.2:57700",
      "is_source": true,
      "is_subscribed": true
    },
    {
      "peer_id": "eMkV1nWvqZfHRTGTTryKKOS2bVZWP6/Y+7wk/Cl/",
      "peer_addr": "172.17.0.2:57428",
      "is_source": true,
      "is_subscribed": true
    },
    {
      "peer_id": "BEjs/sitvo/mVNAV26uHVsO3vELp8YQ/bZEAEu1R",
      "peer_addr": "172.17.0.2:57698",
      "is_source": true,
      "is_subscribed": true
    },
    {
      "peer_id": "Kt1G0OYb7x9bUCt4vmy52mcy++xSeGTBkRwyze2p",
      "peer_addr": "172.17.0.2:57694",
      "is_source": true,
      "is_subscribed": true
    },
    {
      "peer_id": "P1joFRHobS0KXG2762bKIFQbI+luGyf84gxlFZeB",
      "peer_addr": "10.168.1.224:53455",
      "is_source": true,
      "is_subscribed": true
    },
    {
      "peer_id": "1qOoCFIEvTFrphXYURV0WT2ZdlsytMgZLTi/zyf0",
      "peer_addr": "172.17.0.2:57430",
      "is_source": true,
      "is_subscribed": true
    }
  ],
  "out_bounds": [
    {
      "peer_id": "JUwatzxcyq9fJso9UK82Em7193lmnqb+dVsoPKj2",
      "peer_addr": "127.0.0.1:6615",
      "is_source": true,
      "is_subscribed": true
    }
  ]
}
```
