use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::net::tcp::OwnedReadHalf;
use tokio::sync::mpsc;

use crate::config::RedisConfig;

use super::*;

async fn read_request(
    reader: &mut BufReader<OwnedReadHalf>,
) -> std::io::Result<Option<Vec<String>>> {
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Ok(None);
    }
    let count: usize = line
        .trim_start_matches('*')
        .trim()
        .parse()
        .expect("array header");
    let mut args = Vec::with_capacity(count);
    for _ in 0..count {
        let mut header = String::new();
        reader.read_line(&mut header).await?;
        let length: usize = header
            .trim_start_matches('$')
            .trim()
            .parse()
            .expect("bulk header");
        let mut data = vec![0u8; length];
        reader.read_exact(&mut data).await?;
        let mut terminator = [0u8; 2];
        reader.read_exact(&mut terminator).await?;
        args.push(String::from_utf8_lossy(&data).into_owned());
    }
    Ok(Some(args))
}

async fn fake_redis(listener: TcpListener, commands: mpsc::UnboundedSender<Vec<String>>) {
    let Ok((stream, _)) = listener.accept().await else {
        return;
    };
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    while let Ok(Some(args)) = read_request(&mut reader).await {
        let _ = commands.send(args);
        if write_half.write_all(b"+OK\r\n").await.is_err() {
            break;
        }
    }
}

#[tokio::test]
async fn named_input_connection_sets_client_name_before_subscribing() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (commands, mut received) = mpsc::unbounded_channel();
    tokio::spawn(fake_redis(listener, commands));

    let client = redis::Client::open(format!("redis://127.0.0.1:{port}/0")).unwrap();
    let redis_config: RedisConfig =
        serde_json::from_str(&format!(r#"{{"host":"127.0.0.1","port":{port}}}"#)).unwrap();
    let subscriptions = vec![Subscription::Psubscribe {
        pattern: "*".to_owned(),
        output_prefix: String::new(),
        output_suffix: String::new(),
    }];

    let _pubsub = crate::service::input::connect_input(
        &client,
        &redis_config,
        &subscriptions,
        Some("ConflatedPS-test-i"),
    )
    .await
    .unwrap();

    let mut setname_seen = false;
    loop {
        let Ok(Some(args)) = tokio::time::timeout(Duration::from_secs(5), received.recv()).await
        else {
            break;
        };
        match args.first().map(String::as_str) {
            Some("CLIENT") if args.get(1).map(String::as_str) == Some("SETNAME") => {
                assert_eq!(args.get(2).map(String::as_str), Some("ConflatedPS-test-i"));
                setname_seen = true;
            }
            Some("PSUBSCRIBE") => {
                assert!(
                    setname_seen,
                    "CLIENT SETNAME must be sent before PSUBSCRIBE"
                );
                assert_eq!(args.get(1).map(String::as_str), Some("*"));
                return;
            }
            _ => {}
        }
    }
    panic!("the input connection never sent PSUBSCRIBE");
}
