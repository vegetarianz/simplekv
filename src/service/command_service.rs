use crate::*;

impl CommandService for Hget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get(&self.table, &self.key) {
            Ok(Some(v)) => v.into(),
            Ok(None) => KvError::NotFound(self.table, self.key).into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hgetall {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get_all(&self.table) {
            Ok(v) => v.into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hset {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match self.pair {
            Some(v) => match store.set(&self.table, v.key, v.value.unwrap_or_default()) {
                Ok(Some(v)) => v.into(),
                Ok(None) => Value::default().into(),
                Err(e) => e.into(),
            },
            None => Value::default().into(),
        }
    }
}

impl CommandService for Hmget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        self.keys
            .iter()
            .map(|key| match store.get(&self.table, key) {
                Ok(Some(v)) => v,
                _ => Value::default(),
            })
            .collect::<Vec<_>>()
            .into()
    }
}

impl CommandService for Hmset {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        self.pairs
            .into_iter()
            .map(|Kvpair { key, value }| {
                match store.set(&self.table, key, value.unwrap_or_default()) {
                    Ok(Some(v)) => v,
                    _ => Value::default(),
                }
            })
            .collect::<Vec<_>>()
            .into()
    }
}

impl CommandService for Hdel {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.del(&self.table, &self.key) {
            Ok(Some(v)) => v.into(),
            Ok(None) => Value::default().into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hmdel {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        self.keys
            .iter()
            .map(|key| match store.del(&self.table, key) {
                Ok(Some(v)) => v,
                _ => Value::default(),
            })
            .collect::<Vec<_>>()
            .into()
    }
}

impl CommandService for Hexist {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.contains(&self.table, &self.key) {
            Ok(v) => Value::from(v).into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hmexist {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        self.keys
            .iter()
            .map(|key| match store.contains(&self.table, key) {
                Ok(v) => v.into(),
                Err(_) => Value::default(),
            })
            .collect::<Vec<_>>()
            .into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hset_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("t1", "hello", "world".into());
        let res = dispatch(cmd.clone(), &store);
        assert_res_ok(res, &[Value::default()], &[]);

        let res = dispatch(cmd, &store);
        assert_res_ok(res, &["world".into()], &[]);
    }

    #[test]
    fn hget_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("score", "u1", 10.into());
        dispatch(cmd, &store);
        let cmd = CommandRequest::new_hget("score", "u1");
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[10.into()], &[]);
    }

    #[test]
    fn hmget_should_work() {
        let store = MemTable::new();
        set_key_pairs("user", vec![("u1", "s1"), ("u2", "s2")], &store);
        let cmd = CommandRequest::new_hmget("user", vec!["u1".into(), "u2".into(), "u3".into()]);
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &["s1".into(), "s2".into(), Value::default()], &[]);
    }

    #[test]
    fn hmset_should_work() {
        let store = MemTable::new();
        set_key_pairs("user", vec![("u1", "s1"), ("u2", "s2")], &store);
        let pairs = vec![
            Kvpair::new("u1", "ns1".into()),
            Kvpair::new("u2", "ns2".into()),
            Kvpair::new("u3", "ns3".into()),
        ];
        let cmd = CommandRequest::new_hmset("user", pairs);
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &["s1".into(), "s2".into(), Value::default()], &[]);
    }

    #[test]
    fn hdel_should_work() {
        let store = MemTable::new();
        set_key_pairs("user", vec![("u1", "s1")], &store);
        let cmd = CommandRequest::new_hdel("user", "u1");
        let res = dispatch(cmd.clone(), &store);
        assert_res_ok(res, &["s1".into()], &[]);

        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[Value::default()], &[]);
    }

    #[test]
    fn hmdel_should_work() {
        let store = MemTable::new();
        set_key_pairs("user", vec![("u1", "s1")], &store);
        let cmd = CommandRequest::new_hmdel("user", vec!["u1".into(), "u2".into()]);
        let res = dispatch(cmd.clone(), &store);
        assert_res_ok(res, &["s1".into(), Value::default()], &[]);
    }

    #[test]
    fn hexist_should_work() {
        let store = MemTable::new();
        set_key_pairs("user", vec![("u1", "s1")], &store);
        let cmd = CommandRequest::new_hexist("user", "u1");
        let res = dispatch(cmd.clone(), &store);
        assert_res_ok(res, &[true.into()], &[]);

        let cmd = CommandRequest::new_hexist("user", "u2");
        let res = dispatch(cmd.clone(), &store);
        assert_res_ok(res, &[false.into()], &[]);
    }

    #[test]
    fn hmexist_should_work() {
        let store = MemTable::new();
        set_key_pairs("user", vec![("u1", "s1"), ("u2", "s2")], &store);
        let cmd = CommandRequest::new_hmexist("user", vec!["u1".into(), "u2".into(), "u3".into()]);
        let res = dispatch(cmd.clone(), &store);
        assert_res_ok(res, &[true.into(), true.into(), false.into()], &[]);
    }

    fn set_key_pairs(table: &str, pairs: Vec<(&str, impl Into<Value>)>, store: &impl Storage) {
        pairs
            .into_iter()
            .map(|(k, v)| CommandRequest::new_hset(table, k, v.into()))
            .for_each(|cmd| {
                dispatch(cmd, store);
            });
    }

    #[test]
    fn hget_with_non_exist_key_should_return_404() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hget("score", "u1");
        let res = dispatch(cmd, &store);
        assert_res_error(res, 404, "Not found");
    }

    #[test]
    fn hgetall_should_work() {
        let store = MemTable::new();
        let cmds = vec![
            CommandRequest::new_hset("score", "u1", 10.into()),
            CommandRequest::new_hset("score", "u2", 8.into()),
            CommandRequest::new_hset("score", "u3", 11.into()),
            CommandRequest::new_hset("score", "u1", 6.into()),
        ];
        for cmd in cmds {
            dispatch(cmd, &store);
        }

        let cmd = CommandRequest::new_hgetall("score");
        let res = dispatch(cmd, &store);
        let pairs = &[
            Kvpair::new("u1", 6.into()),
            Kvpair::new("u2", 8.into()),
            Kvpair::new("u3", 11.into()),
        ];
        assert_res_ok(res, &[], pairs);
    }
}
