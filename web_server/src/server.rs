use may_minihttp::{HttpService, Request, Response};

use super::config;

use std::{
    fs::File,
    io::{self, prelude::*},
    path::{Path, PathBuf},
};

pub struct Server;

impl HttpService for Server {
    fn call(&self, request: Request) -> io::Result<Response> {
        let uri_path = request.path();
        let mut resp = Response::new();

        if let Some(path) = local_path_for_request(&uri_path, &config::get_root_dir().unwrap()) {
            let mime_type = file_path_mime(&path);

            let mut file = File::open(path)?;
            let mut buffer = Vec::new();

            // read the whole file
            file.read_to_end(&mut buffer)?;
            let content = unsafe { String::from_utf8_unchecked(buffer) };
            resp.header("Content_Type", &mime_type);
            resp.body(&content);

            return Ok(resp);
        }

        Ok(resp)
    }
}

fn file_path_mime(file_path: &Path) -> String {
    String::from(
        match file_path.extension().and_then(std::ffi::OsStr::to_str) {
            Some("html") => "text/html",
            Some("css") => "text/css",
            Some("js") => "text/javascript",
            Some("jpg") => "image/jpeg",
            Some("png") => "image/png",
            Some("wasm") => "application/wasm",
            _ => "text/plain",
        },
    )
}

fn local_path_for_request(request_path: &str, root_dir: &Path) -> Option<PathBuf> {
    // This is equivalent to checking for hyper::RequestUri::AbsoluteUri
    if !request_path.starts_with("/") {
        return None;
    }

    // Trim off the url parameters starting with '?'
    let end = request_path.find('?').unwrap_or(request_path.len());
    let request_path = &request_path[0..end];

    // Append the requested path to the root directory
    let mut path = root_dir.to_owned();
    if request_path.starts_with('/') {
        path.push(&request_path[1..]);
    } else {
        return None;
    }

    // Maybe turn directory requests into index.html requests
    if request_path.ends_with('/') {
        path.push("index.html");
    }

    Some(path)
}
