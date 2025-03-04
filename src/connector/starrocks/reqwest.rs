use reqwest::blocking::Body;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::error::Error;
    use std::io;
    use std::io::Read;
    use std::sync::Arc;
    use std::time::Duration;
    use base64::Engine;
    use bytes::BytesMut;
    use reqwest::blocking::{Client, Request, Response};
    use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
    use reqwest::{redirect, Url};
    use reqwest::redirect::Policy;
    use super::*;

    struct BytesMutReader {
        data: Arc<BytesMut>,
        pos: usize,
    }

    impl BytesMutReader {
        fn new(data:  Arc<BytesMut>) -> Self {
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
    fn test_http() -> Result<(), Box<dyn Error>>{
        // StarRocks 的 Stream Load 地址
        let url = "http://192.168.216.86:8061/api/test/object_stat/_stream_load";

        // 用户名和密码
        let username = "root";
        let password = "";

        // 构造 Basic 认证信息
        let auth = format!("{}:{}", username, password);
        let auth_base64 = base64::engine::general_purpose::STANDARD.encode(auth);
        let auth_header_value = format!("Basic {}", auth_base64);

        // 构造 HTTP 客户端
        let client = Client::builder().build()?;

        // 发送 Stream Load 请求
        let data = r#"[
        {"timestamp":"2025-03-02 14:19:25","object_id":1},
        {"timestamp":"2025-03-02 14:19:25","object_id":2}
        ]"#; // 示例数据
        println!("{}", data);
        let r = client
            .put(url)
            .header("authorization", "Basic cm9vdDo=")
            //.basic_auth(username, Some(password))
            .header("Expect", "100-continue")
            .header("two_phase_commit", "false")
            .header("format", "json")
            .header("strip_outer_array", "true")
            .header("ignore_json_size", "true");
        println!("{:#?}", r);
        let response = r.body(data)
            .send()?;

        // 检查响应
        if response.status().is_success() {
            println!("Stream Load 成功: {}", response.text()?);
        } else {
            println!("Stream Load 失败: {:?}", response);
        }

        Ok(())
    }

    fn send_stream_load_request(
        client: &Client,
        mut request: Request,
        fe_host: &str,
    ) -> Result<Response, Box<dyn std::error::Error>> {
        let max_redirects = 2; // 最大重定向次数
        let mut redirects = 0;

        loop {
            if redirects >= max_redirects {
                return Err("Exceeded maximum redirect attempts".into());
            }

            // 克隆请求以保留原始请求对象
            let mut request_for_redirection = request
                .try_clone()
                .ok_or_else(|| "Failed to clone request".to_string())?;

            eprintln!("request: {:#?}", request_for_redirection);
            // 执行当前请求
            let response = client.execute(request_for_redirection)?;
            eprintln!("response: {:#?}", response);
            //eprintln!("response: {}", response.text()?);


            // 获取原始请求的 HTTP 端口
            let original_http_port = request.url().port();

            match try_get_be_url(&response, fe_host)? {
                Some(be_url) => {
                    // 更新请求 URL
                    *request.url_mut() = be_url.clone();

                    // 检查是否重定向到 FE 或 BE
                    if is_fe_redirect(&be_url, request.url()) {
                        // 重定向到 FE，继续下一轮
                        redirects += 1;
                        println!("Redirecting to FE: {}", be_url);
                    } else {
                        // 重定向到 BE，返回响应
                        println!("Redirecting to BE: {}", be_url);
                        return Ok(response);
                    }
                }
                None => {
                    // 没有重定向，直接返回响应
                    return Ok(response);
                }
            }
        }
    }

    fn send_stream_load_request2(
        client: &Client,
        mut request: Request,
        fe_host: &str,
    ) -> Result<Response, Box<dyn std::error::Error>> {
        let max_redirects = 2; // 最大重定向次数
        let mut redirects = 0;

        //let body = mem::replace(request.body_mut(), None);
        let body =r#"[
        {"timestamp":"2025-03-02 14:39:25","object_id":1},
        {"timestamp":"2025-03-02 14:39:25","object_id":2}
        ]"#;
        let bytes_mut = Arc::new(BytesMut::from(body));

        loop {
            if redirects >= max_redirects {
                return Err("Exceeded maximum redirect attempts".into());
            }

            // 克隆请求以保留原始请求对象
            let mut request_for_redirection = Request::new(request.method().clone(), request.url().clone());
            *request_for_redirection.headers_mut() = request.headers().clone();
            *request_for_redirection.version_mut() = request.version().clone();
            *request_for_redirection.body_mut() = Some(Body::new(BytesMutReader::new(bytes_mut.clone())));

            eprintln!("request: {:#?}", request_for_redirection);
            // 执行当前请求
            let response = client.execute(request_for_redirection)?;
            eprintln!("response: {:#?}", response);
            //eprintln!("response: {}", response.text()?);


            // 获取原始请求的 HTTP 端口
            let original_http_port = request.url().port();

            match try_get_be_url(&response, fe_host)? {
                Some(be_url) => {
                    // 更新请求 URL
                    *request.url_mut() = be_url.clone();

                    // 检查是否重定向到 FE 或 BE
                    if is_fe_redirect(&be_url, request.url()) {
                        // 重定向到 FE，继续下一轮
                        redirects += 1;
                        println!("Redirecting to FE: {}", be_url);
                    } else {
                        // 重定向到 BE，返回响应
                        println!("Redirecting to BE: {}", be_url);
                        return Ok(response);
                    }
                }
                None => {
                    // 没有重定向，直接返回响应
                    return Ok(response);
                }
            }
        }
    }

    /// 尝试从重定向响应中获取 BE 的 URL
    fn try_get_be_url(resp: &Response, fe_host: &str) -> Result<Option<Url>, Box<dyn std::error::Error>> {
        match resp.status() {
            reqwest::StatusCode::TEMPORARY_REDIRECT => {
                if let Some(location) = resp.headers().get("location") {
                    let location_str = location.to_str()?;
                    let mut parsed_url = Url::parse(location_str)?;

                    // 如果目标主机是 localhost 或 127.0.0.1，则替换为目标 FE 主机地址
                    if let Some(host) = parsed_url.host_str() {
                        if host == "127.0.0.1" || host == "localhost" {
                            parsed_url.set_host(Some(fe_host))?;
                        }
                    }

                    Ok(Some(parsed_url))
                } else {
                    Err("Redirect response missing 'Location' header".into())
                }
            }
            reqwest::StatusCode::OK => Ok(None), // 没有重定向
            status => Err(format!("Unexpected status code: {}", status).into()),
        }
    }

    /// 检查是否重定向到 FE
    fn is_fe_redirect(new_url: &Url, original_url: &Url) -> bool {
        new_url.port() == original_url.port()
    }

    #[test]
    fn test_stream_load_redirect2() {
        // 创建 HTTP 客户端
        let client = Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap();

        // 构造初始请求
        let host = "http://192.168.216.86:8061";
        let database = "test";
        let table = "object_stat";
        let username = "root";
        let password = "";

        // 生成 Basic Auth 的 Header
        let auth = format!("{}:{}", username, password);
        let encoded_auth = base64::encode(auth);
        let auth_header = format!("Basic {}", encoded_auth);

        // 构造请求 URL
        let url = format!("{}/api/{}/{}/_stream_load", host, database, table);

        // 构造请求体
        let body =r#"[
        {"timestamp":"2025-03-02 14:39:25","object_id":1},
        {"timestamp":"2025-03-02 14:39:25","object_id":2}
        ]"#; // 示例数据

        // 构造请求头部
        let mut headers = HashMap::new();
        headers.insert("Authorization", auth_header);
        headers.insert("Expect", "100-continue".to_string());
        headers.insert("two_phase_commit", "false".to_string());
        headers.insert("format", "json".to_string());
        headers.insert("ignore_json_size", "true".to_string());
        headers.insert("strip_outer_array", "true".to_string());

        // 构造请求
        let request = client
            .put(&url)
            .headers(reqwest::header::HeaderMap::from_iter(headers.into_iter().map(|(k, v)| {
                (
                    reqwest::header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                    reqwest::header::HeaderValue::from_str(&v).unwrap(),
                )
            })))
            .body(body)
            .build().unwrap();

        // 发送请求并处理重定向
        let response = match send_stream_load_request2(&client, request, "192.168.216.86") {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Error: {:#?}", e);
                return;
            },
        };

        // 检查响应内容
        if response.status().is_success() {
            println!("Stream Load 成功: {}", response.text().unwrap());
        } else {
            eprintln!("Stream Load 失败: {:#?}", response);
        }

    }

    #[test]
    fn test_stream_load_redirect() {
        // 创建 HTTP 客户端
        let client = Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap();

        // 构造初始请求
        let host = "http://192.168.216.86:8061";
        let database = "test";
        let table = "object_stat";
        let username = "root";
        let password = "";

        // 生成 Basic Auth 的 Header
        let auth = format!("{}:{}", username, password);
        let encoded_auth = base64::encode(auth);
        let auth_header = format!("Basic {}", encoded_auth);

        // 构造请求 URL
        let url = format!("{}/api/{}/{}/_stream_load", host, database, table);

        // 构造请求体
        let body =r#"[
        {"timestamp":"2025-03-02 14:39:25","object_id":1},
        {"timestamp":"2025-03-02 14:39:25","object_id":2}
        ]"#; // 示例数据

        // 构造请求头部
        let mut headers = HashMap::new();
        headers.insert("Authorization", auth_header);
        headers.insert("Expect", "100-continue".to_string());
        headers.insert("two_phase_commit", "false".to_string());
        headers.insert("format", "json".to_string());
        headers.insert("ignore_json_size", "true".to_string());
        headers.insert("strip_outer_array", "true".to_string());

        // 构造请求
        let request = client
            .put(&url)
            .headers(reqwest::header::HeaderMap::from_iter(headers.into_iter().map(|(k, v)| {
                (
                    reqwest::header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                    reqwest::header::HeaderValue::from_str(&v).unwrap(),
                )
            })))
            .body(body)
            .build().unwrap();

        // 发送请求并处理重定向
        let response = match send_stream_load_request(&client, request, "192.168.216.86") {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Error: {:#?}", e);
                return;
            },
        };

        // 检查响应内容
        if response.status().is_success() {
            println!("Stream Load 成功: {}", response.text().unwrap());
        } else {
            eprintln!("Stream Load 失败: {:#?}", response);
        }

    }


}