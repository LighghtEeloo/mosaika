use mosaika::syntax;
use schemars::schema_for;

pub fn main() {
    let schema = schema_for!(syntax::Proj);
    println!("{}", serde_json::to_string_pretty(&schema).unwrap());
}
