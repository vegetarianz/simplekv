mod command_service;

use std::sync::Arc;

use crate::command_request::RequestData;
use crate::storage::{MemTable, Storage};
use crate::KvError;
use crate::{CommandRequest, CommandResponse};

use tracing::debug;

/// 对 Command 的处理的抽象
pub trait CommandService {
    /// 处理 Command，返回 Response
    fn execute(self, store: &impl Storage) -> CommandResponse;
}

/// Service 数据结构
pub struct Service<Store = MemTable> {
    inner: Arc<ServiceInner<Store>>,
}

impl<Store> Clone for Service<Store> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<Store> From<ServiceInner<Store>> for Service<Store> {
    fn from(inner: ServiceInner<Store>) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl<Store: Storage> Service<Store> {
    pub fn execute(&self, cmd: CommandRequest) -> CommandResponse {
        debug!("Got request: {:?}", cmd);
        // 发送 on_received 事件
        self.inner.on_received.notify(&cmd);
        // 执行命令获取响应
        let mut res = dispatch(cmd, &self.inner.store);

        debug!("Executed response: {:?}", res);
        // 发送 on_executed 事件
        self.inner.on_executed.notify(&res);
        // 发送 on_before_send 事件
        self.inner.on_before_send.notify(&mut res);

        res
    }
}

/// Service 内部数据结构
pub struct ServiceInner<Store> {
    store: Store,
    on_received: Vec<fn(&CommandRequest)>,
    on_executed: Vec<fn(&CommandResponse)>,
    on_before_send: Vec<fn(&mut CommandResponse)>,
    on_after_send: Vec<fn()>,
}

impl<Store> ServiceInner<Store> {
    pub fn new(store: Store) -> Self {
        Self {
            store,
            on_received: Vec::new(),
            on_executed: Vec::new(),
            on_before_send: Vec::new(),
            on_after_send: Vec::new(),
        }
    }

    #[allow(dead_code)]
    fn fn_received(mut self, on_received: fn(&CommandRequest)) -> Self {
        self.on_received.push(on_received);
        self
    }

    #[allow(dead_code)]
    fn fn_executed(mut self, on_executed: fn(&CommandResponse)) -> Self {
        self.on_executed.push(on_executed);
        self
    }

    #[allow(dead_code)]
    fn fn_before_send(mut self, on_before_send: fn(&mut CommandResponse)) -> Self {
        self.on_before_send.push(on_before_send);
        self
    }

    #[allow(dead_code)]
    fn fn_after_send(mut self, on_after_send: fn()) -> Self {
        self.on_after_send.push(on_after_send);
        self
    }
}

// 处理 Request 得到 Response
pub fn dispatch(cmd: CommandRequest, store: &impl Storage) -> CommandResponse {
    match cmd.request_data {
        Some(RequestData::Hget(param)) => param.execute(store),
        Some(RequestData::Hgetall(param)) => param.execute(store),
        Some(RequestData::Hset(param)) => param.execute(store),
        Some(RequestData::Hmget(param)) => param.execute(store),
        Some(RequestData::Hmset(param)) => param.execute(store),
        Some(RequestData::Hdel(param)) => param.execute(store),
        Some(RequestData::Hmdel(param)) => param.execute(store),
        Some(RequestData::Hexist(param)) => param.execute(store),
        Some(RequestData::Hmexist(param)) => param.execute(store),
        None => KvError::InvalidCommand("Request has no data".into()).into(),
    }
}

trait Notify<Args> {
    fn notify(&self, args: &Args);
}

trait NotifyMut<Args> {
    fn notify(&self, args: &mut Args);
}

impl<Args> Notify<Args> for Vec<fn(&Args)> {
    fn notify(&self, args: &Args) {
        for f in self {
            f(args);
        }
    }
}

impl<Args> NotifyMut<Args> for Vec<fn(&mut Args)> {
    fn notify(&self, args: &mut Args) {
        for f in self {
            f(args);
        }
    }
}

#[cfg(test)]
mod tests {
    use http::StatusCode;
    use tracing::info;

    use super::*;
    use crate::{MemTable, Value};
    use std::thread;

    #[test]
    fn event_registration_should_work() {
        fn on_received(cmd: &CommandRequest) {
            info!("on_received: {:?}", cmd);
        }

        fn on_executed(res: &CommandResponse) {
            info!("on_executed: {:?}", res);
        }

        fn on_before_send(res: &mut CommandResponse) {
            res.status = StatusCode::CREATED.as_u16() as _;
        }

        fn on_after_send() {
            info!("on_after_send");
        }

        let service: Service = ServiceInner::new(MemTable::default())
            .fn_received(on_received)
            .fn_executed(on_executed)
            .fn_before_send(on_before_send)
            .fn_after_send(on_after_send)
            .into();

        let res = service.execute(CommandRequest::new_hset("user", "u1", "s1".into()));
        assert_eq!(res.status, StatusCode::CREATED.as_u16() as _);
        assert_eq!(res.message, "");
        assert_eq!(res.values, &[Value::default()]);
    }

    #[test]
    fn service_should_works() {
        // 我们需要一个 service 结构至少包含 Storage
        let service: Service = ServiceInner::new(MemTable::default()).into();

        // service 可以运行在多线程环境下，它的 clone 应该是轻量级的
        let cloned = service.clone();

        // 创建一个线程，在 table t1 中写入 k1, v1
        let handle = thread::spawn(move || {
            let res = cloned.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
            assert_res_ok(res, &[Value::default()], &[]);
        });
        handle.join().unwrap();

        // 在当前线程下读取 table t1 的 k1，应该返回 v1
        let res = service.execute(CommandRequest::new_hget("t1", "k1"));
        assert_res_ok(res, &["v1".into()], &[]);
    }
}

#[cfg(test)]
use crate::{Kvpair, Value};

// 测试成功返回的结果
#[cfg(test)]
pub fn assert_res_ok(mut res: CommandResponse, values: &[Value], pairs: &[Kvpair]) {
    res.pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(res.status, 200);
    assert_eq!(res.message, "");
    assert_eq!(res.values, values);
    assert_eq!(res.pairs, pairs);
}

// 测试失败返回的结果
#[cfg(test)]
pub fn assert_res_error(res: CommandResponse, code: u32, msg: &str) {
    assert_eq!(res.status, code);
    assert!(res.message.contains(msg));
    assert_eq!(res.values, &[]);
    assert_eq!(res.pairs, &[]);
}
