


#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::error::Error;
    use std::io;
    use std::io::Read;
    use std::time::Duration;
    use bytes::BytesMut;
    use isahc::{prelude::*, HttpClient, Request, config::RedirectPolicy, Body};

    struct BytesMutReader {
        data: BytesMut,
        pos: usize,
    }

    impl BytesMutReader {
        fn new(data: BytesMut) -> Self {
            Self { data, pos: 0 }
        }
    }

    impl Read for BytesMutReader {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if self.pos >= self.data.len() {
                return Ok(0); // 读取完毕
            }

            let len = std::cmp::min(buf.len(), self.data.len() - self.pos);
            buf[..len].copy_from_slice(&self.data[self.pos..self.pos + len]);
            self.pos += len;

            Ok(len)
        }
    }

    #[test]
    fn test_http3() -> crate::Result<(), Box<dyn Error>> {
        let starrocks_host = "http://192.168.216.86:8061";
        let database = "test";
        let table = "object_stat";

        let url = format!("{}/api/{}/{}/_stream_load", starrocks_host, database, table);

        // 构造 JSON 数据
        let json_data = r#"[
        {"timestamp":"2025-03-02 14:19:25","object_id":1},
        {"timestamp":"2025-03-02 14:19:25","object_id":2}
        ]"#;
        let data = BytesMut::from(json_data);
        let mut reader = BytesMutReader::new(data);

        let client = HttpClient::builder()
            .redirect_policy(RedirectPolicy::Follow)
            .timeout(Duration::from_secs(60))
            .default_headers(HashMap::from([
                ("authorization", "Basic cm9vdDo="),
                ("Expect", "100-continue"),
                ("two_phase_commit", "false"),
                ("format", "json"),
                ("strip_outer_array", "true"),
                ("ignore_json_size", "true"),
            ]))
            .build()?;

        let mut response = client.put(url, Body::from_reader(reader))?;

        // 处理响应
        println!("Response Status: {}", response.status());
        let body = response.text()?;
        println!("Response Body: {}", body);

        Ok(())
    }

    #[test]
    fn test_http2() -> crate::Result<(), Box<dyn Error>> {
        let starrocks_host = "http://192.168.216.86:8061";
        let database = "test";
        let table = "object_stat";

        let url = format!("{}/api/{}/{}/_stream_load", starrocks_host, database, table);

        // 构造 JSON 数据
        let json_data = r#"[
        {"timestamp":"2025-03-02 14:19:25","object_id":1},
        {"timestamp":"2025-03-02 14:19:25","object_id":2}
        ]"#;

        let client = HttpClient::builder()
            .redirect_policy(RedirectPolicy::Follow)
            .timeout(Duration::from_secs(60))
            .default_headers(HashMap::from([
                ("authorization", "Basic cm9vdDo="),
                ("Expect", "100-continue"),
                ("two_phase_commit", "false"),
                ("format", "json"),
                ("strip_outer_array", "true"),
                ("ignore_json_size", "true"),
            ]))
            .build()?;

        let mut response = client.put(url, json_data)?;
        /*.header("authorization", "Basic cm9vdDo=")
        //.basic_auth(username, Some(password))
        .header("Expect", "100-continue")
        .header("two_phase_commit", "false")
        .header("format", "json")
        .header("strip_outer_array", "true")
        .header("ignore_json_size", "true")
        .send()?;*/

        // 处理响应
        println!("Response Status: {}", response.status());
        let body = response.text()?;
        println!("Response Body: {}", body);

        Ok(())
    }

    #[test]
    fn test_http() -> crate::Result<(), Box<dyn Error>> {
        let starrocks_host = "http://192.168.216.86:8061";
        let database = "test";
        let table = "object_stat";

        let url = format!("{}/api/{}/{}/_stream_load", starrocks_host, database, table);

        // 构造 JSON 数据
        let json_data = r#"[
        {"timestamp":"2025-03-02 14:19:25","object_id":1},
        {"timestamp":"2025-03-02 14:19:25","object_id":2}
        ]"#;

        let mut response = Request::put(url)
            .header("authorization", "Basic cm9vdDo=")
            .header("Expect", "100-continue")
            .header("two_phase_commit", "false")
            .header("format", "json")
            .header("strip_outer_array", "true")
            .header("ignore_json_size", "true")
            .body(json_data.to_string())?
            .send()?;

        // 处理响应
        println!("Response Status: {}", response.status());
        let body = response.text()?;
        println!("Response Body: {}", body);

        Ok(())
    }
}