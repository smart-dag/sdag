extern crate criterion;
extern crate sdag;
extern crate serde_json;

use criterion::*;
use sdag::error::Result;
use sdag::joint::Joint;
use sdag::kv_store::KV_STORE;

static JOINT: &str = r#"{
        "unit":{
            "alt":"1",
            "authors":[
                {
                    "address":"LWFAESN3EB5E5VFXJ7JWIJB7K5MDQCZE",
                    "authentifiers":{
                        "r":"l412FzG4ZMESwMASqNdNfXhj2XvSGhOblud5DuKhbc8mnNJFFxpTLUU0s3SuDL8ONLQ1OaWQHN7lTx8B53Ofqw=="
                    }
                }
            ],
            "headers_commission":344,
            "last_ball":"n/7WqfyUwX14nS/+Iw2O4LvivSqwVecPkSpl8qbUvJM=",
            "last_ball_unit":"Gz0nOu5Utp3WtCZwlfG5+TbqRMGvF8fDsAVWh9BJc7Q=",
            "messages":[
                {
                    "app":"payment",
                    "payload":{
                        "inputs":[
                            {
                                "message_index":1,
                                "output_index":41,
                                "unit":"Gz0nOu5Utp3WtCZwlfG5+TbqRMGvF8fDsAVWh9BJc7Q="
                            }
                        ],
                        "outputs":[
                            {
                                "address":"LWFAESN3EB5E5VFXJ7JWIJB7K5MDQCZE",
                                "amount":999499
                            }
                        ]
                    },
                    "payload_hash":"LRsKHh5DMb30BLrPBlY81vLdFcIr0JboraHoN15pjfM=",
                    "payload_location":"inline"
                }
            ],
            "parent_units":[
                "BQFT9TpXhXbxd0b+rqGeBvehuJjnrV+NjA7Alp4IkHM="
            ],
            "payload_commission":157,
            "timestamp":1547396486,
            "unit":"MHBF65OZbRHOEVyicHo7DUfUjxt41ILtQ7f7QAwBPGc=",
            "version":"1.0",
            "witness_list_unit":"Gz0nOu5Utp3WtCZwlfG5+TbqRMGvF8fDsAVWh9BJc7Q="
        }
    }"#;

fn kv_store_read_joint(key: &str) -> Result<()> {
    let _ = KV_STORE.read_joint(key)?;
    Ok(())
}

fn kv_store_save_joint(joint: &Joint) -> Result<()> {
    KV_STORE.save_joint(&joint.unit.unit, &joint)?;
    Ok(())
}

fn kv_store_update_joint(joint: &Joint) -> Result<()> {
    KV_STORE.update_joint(&joint.unit.unit, &joint)?;
    Ok(())
}

fn criterion_benchmark(c: &mut Criterion) {
    let joint: Joint = serde_json::from_str(JOINT).expect("string to joint error");
    KV_STORE
        .save_joint(&joint.unit.unit, &joint)
        .expect("save joint error");
    c.bench_function("kv store read joint", move |b| {
        b.iter(|| kv_store_read_joint(&joint.unit.unit))
    });

    let joint: Joint = serde_json::from_str(JOINT).expect("string to joint error");
    c.bench_function("kv store save joint", move |b| {
        b.iter_batched(
            || joint.clone(),
            |joint| kv_store_save_joint(&joint),
            BatchSize::PerIteration,
        )
    });

    let joint: Joint = serde_json::from_str(JOINT).expect("string to joint error");
    c.bench_function("kv store update joint", move |b| {
        b.iter_batched(
            || joint.clone(),
            |joint| kv_store_update_joint(&joint),
            BatchSize::PerIteration,
        )
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
