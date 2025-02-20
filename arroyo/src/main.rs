use std::fs;
use std::hint::black_box;
use std::io::BufReader;
use std::sync::Arc;
use arrow::datatypes::{Field, Schema, SchemaRef, TimeUnit};

fn nexmark_schema() -> SchemaRef {
    use arrow::datatypes::DataType::*;

    let person_fields = vec![
        Field::new("id", Int64, false),
        Field::new("name", Utf8, false),
        Field::new("email_address", Utf8, false),
        Field::new("credit_card", Utf8, false),
        Field::new("city", Utf8, false),
        Field::new("state", Utf8, false),
        Field::new("datetime", Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("extra", Utf8, false),
    ];

    let auction_fields = vec![
        Field::new("id", Int64, false),
        Field::new("description", Utf8, false),
        Field::new("item_name", Utf8, false),
        Field::new("initial_bid", Int64, false),
        Field::new("reserve", Int64, false),
        Field::new("datetime", Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("expires", Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("seller", Int64, false),
        Field::new("category", Int64, false),
        Field::new("extra", Utf8, false),
    ];

    Arc::new(Schema::new(vec![
        Field::new("person", Struct(person_fields.into()), true),
        Field::new("auction", Struct(auction_fields.into()), true),
        Field::new("bid", Struct(bid_schema().fields.clone().into()), true),
    ]))
}

fn bid_schema() -> SchemaRef {
    use arrow::datatypes::DataType::*;

    Arc::new(Schema::new(  vec![
        Field::new("auction", Int64, false),
        Field::new("bidder", Int64, false),
        Field::new("price", Int64, false),
        Field::new("channel", Utf8, false),
        Field::new("url", Utf8, false),
        Field::new("datetime", Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("extra", Utf8, false),
    ]))
}

fn tweet_schema() -> SchemaRef {
    use arrow::datatypes::DataType::*;
     
    Arc::new(Schema::new(vec![
        Field::new("metadata", Struct(vec![
            Field::new("result_type", Utf8, false),
            Field::new("iso_language_code", Utf8, false),
        ].into()), false),
        Field::new("created_at", Utf8, false),
        Field::new("id", Int64, false),
        Field::new("id_str", Utf8, false),
        Field::new("text", Utf8, false),
        Field::new("source", Utf8, false),
        Field::new("truncated", Boolean, false),
        Field::new("in_reply_to_status_id", Int64, true),
        Field::new("in_reply_to_status_id_str", Utf8, true),
        Field::new("in_reply_to_user_id", Int64, true),
        Field::new("in_reply_to_user_id_str", Utf8, true),
        Field::new("in_reply_to_screen_name", Utf8, true),
        Field::new("user", Struct(vec![
            Field::new("id", Int64, false),
            Field::new("id_str", Utf8, false),
            Field::new("name", Utf8, false),
            Field::new("screen_name", Utf8, false),
            Field::new("location", Utf8, true),
            Field::new("description", Utf8, true),
            Field::new("url", Utf8, true),
            Field::new("followers_count", Int32, false),
            Field::new("friends_count", Int32, false),
            Field::new("listed_count", Int32, false),
            Field::new("created_at", Utf8, false),
            Field::new("favourites_count", Int32, false),
        ].into()), false),
        Field::new("retweet_count", Int32, false),
        Field::new("favorite_count", Int32, false),
        Field::new("lang", Utf8, false),
    ]))
}

fn log_schema() -> SchemaRef {
    use arrow::datatypes::DataType::*;
    
    Arc::new(Schema::new(
        vec![
            Field::new("ip", Utf8, false ),
            Field::new("identity", Utf8, false),
            Field::new("user_id", Utf8, false),
            Field::new("timestamp", Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("date", Utf8, false),
            Field::new("request", Utf8, false),
            Field::new("code", UInt32, false),
            Field::new("size", UInt64, false),
        ]
    ))
}

fn main() {
    let file = fs::read_to_string(std::env::current_dir().unwrap().join("../data/nexmark/nexmark.json")).unwrap();

    let mut reader = arrow_json::reader::ReaderBuilder::new(nexmark_schema())
        //.with_limit_to_batch_size(false)
        .with_strict_mode(false)
        //.with_batch_size(1024)
        .build_decoder()
        .unwrap();

    for _ in 0..10000 {
        reader.decode(file.as_bytes()).unwrap();

        let record_batch = reader.flush().unwrap().unwrap();
        //assert_eq!(record_batch.num_rows(), 100);
    }
}
