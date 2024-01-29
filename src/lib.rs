extern crate serde;
extern crate serde_json;

use pyo3::prelude::*;
use mobc_redis::{mobc, RedisConnectionManager, redis::Client};
use std::env;
use mobc_redis::redis::AsyncCommands;

use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use serde_json::Error;

#[pyclass]
struct RedisPool {
    pool: mobc::Pool<RedisConnectionManager>,
}

#[derive(Serialize, Deserialize, Default)]
struct A1 {
    c1: String,
    c2: String,
    c3: String,
    c4: String,
    c5: String,
    c6: String,
}

impl A1 {
    fn parse(&mut self, records: &[(String, String)]) -> Result<String, Error> {
        let split_1 = env::var("RS_SPLIT_1").unwrap_or(";".to_string());
        let split_2 = env::var("RS_SPLIT_2").unwrap_or("|".to_string());

        let mut hash_map: HashMap<String, Vec<A1>> = HashMap::new();
        for (key, value) in records.into_iter() {
            let mut vec_values = Vec::new();
            for item in value.split(split_1.as_str()) {
                let mut a1 = A1::default();
                let parts: Vec<&str> = item.split(split_2.as_str()).collect();
                a1.c1 = parts[0].to_string();
                a1.c2 = parts[1].to_string();
                a1.c3 = parts[2].to_string();
                a1.c4 = parts[3].to_string();
                a1.c5 = parts[4].to_string();
                a1.c6 = parts[5].to_string();
                vec_values.push(a1);
            }
            hash_map.insert(key.to_string(), vec_values);
        }
        let mut a1_map: HashMap<String, HashMap<String, Vec<A1>>> = HashMap::new();
        a1_map.insert("a1".to_string(), hash_map);
        let json_str = serde_json::to_string(&a1_map).unwrap();
        Ok(json_str)
    }
}

#[pymethods]
impl RedisPool {
    /// 创建redis的连接池
    #[new]
    fn new(py: Python) -> PyResult<Self> {
        py.allow_threads(move || {
            match env::var("RS_URL") {
                Ok(url) => {
                    let client = Client::open(url).map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(format!("ConnectionManager Error: {}", e)))?;
                    let manager = RedisConnectionManager::new(client);
                    let pool = mobc::Pool::builder().build(manager);
                    Ok(RedisPool { pool })
                }
                Err(_) => {
                    Err(PyErr::new::<pyo3::exceptions::PyException, _>("`RS_URL` system variable not found"))
                }
            }
        })
    }

    /// 插入redis的hash
    /// items 的形式 [("field_1","value_1"),("field_2","value_2"),...]
    fn insert(&self, py: Python, key: &str, items: Vec<(&str, &str)>) -> PyResult<()> {
        py.allow_threads(move || {
            let pool = self.pool.clone();
            async_std::task::block_on(async move {
                let mut conn = pool.get().await.map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Pool Get Connection Error: {}", e)))?;
                conn.hset_multiple(key, items.as_slice()).await.map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("`HMSET` Command Error: {}", e)))?;
                Ok(())
            })
        })
    }

    /// 获得插入mysql的结构化字段
    fn get_all(&self, py: Python, key: &str) -> PyResult<Vec<(String, String)>> {
        py.allow_threads(move || {
            let pool = self.pool.clone();
            async_std::task::block_on(async move {
                let mut conn = pool.get().await.map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Pool Get Connection Error: {}", e)))?;
                let mut result: Vec<(String, String)> = conn.hgetall(key).await.map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("`HGETALL` Command Error: {}", e)))?;
                result.sort_by(|a, b| b.0.cmp(&a.0));
                Ok(result)
            })
        })
    }

    /// 插入redis的hash会进行格式化
    /// items 的形式 [
    ///     [["c00","c01","c02","c03","c04","c05"],["c06","c07","c08","c09","c010","c011"],...],
    ///     [["c10","c11","c12","c13","c14","c15"],["c16","c17","c18","c19","c110","c111"],...],
    ///     ...
    /// ]
    /// "c00","c01",...: 无意义只是占位符
    fn insert_imgs(&self, py: Python, key: &str, items: Vec<Vec<Vec<&str>>>) -> PyResult<()> {
        let split_1 = env::var("RS_SPLIT_1").unwrap_or(";".to_string());
        let split_2 = env::var("RS_SPLIT_2").unwrap_or("|".to_string());
        py.allow_threads(move || {
            let pool = self.pool.clone();
            async_std::task::block_on(async move {
                let mut conn = pool.get().await.map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Pool Get Connection Error: {}", e)))?;
                let mut insert_vec: Vec<(String, String)> = Vec::new();
                for (index, imgs) in items.iter().enumerate() {
                    let mut tmp_vec: Vec<String> = Vec::new();
                    for img in imgs.iter() {
                        tmp_vec.push(img.join(split_2.as_str()));
                    }
                    insert_vec.push((index.to_string(), tmp_vec.join(split_1.as_str())));
                }
                let imgs_key = format!("{}-{}", key, "imgs");
                conn.hset_multiple(imgs_key.as_str(), insert_vec.as_slice()).await.map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("`HMSET` Command Error: {}", e)))?;
                Ok(())
            })
        })
    }

    /// 获得插入mysql的可变的json字段
    fn get_all_imgs(&self, py: Python, key: &str) -> PyResult<String> {
        match env::var("RS_IS_RECORD") {
            Ok(is_record) => {
                let is_record = is_record.parse::<bool>().unwrap_or(false);
                if is_record {
                    py.allow_threads(move || {
                        let pool = self.pool.clone();
                        async_std::task::block_on(async move {
                            let mut conn = pool.get().await.map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Pool Get Connection Error: {}", e)))?;
                            let imgs_key = format!("{}-{}", key, "imgs");
                            let mut result: Vec<(String, String)> = conn.hgetall(imgs_key.as_str()).await.map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("`HGETALL` Command Error: {}", e)))?;
                            result.sort_by(|a, b| b.0.cmp(&a.0));
                            let mut a1 = A1::default();
                            match env::var("RS_LIMIT") {
                                Ok(limit) => {
                                    let limit = limit.parse::<usize>().unwrap_or(10);
                                    let result = &result[..limit];
                                    let json_str = a1.parse(result).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Json Parse Error: {}", e)))?;
                                    Ok(json_str)
                                }
                                Err(_) => {
                                    let json_str = a1.parse(result.as_slice()).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Json Parse Error: {}", e)))?;
                                    Ok(json_str)
                                }
                            }
                        })
                    })
                } else {
                    Ok(String::default())
                }
            }
            Err(_) => {
                Ok(String::default())
            }
        }
    }

    fn del_all(&self, py: Python, key: &str) -> PyResult<()> {
        py.allow_threads(move || {
            let pool = self.pool.clone();
            async_std::task::block_on(async move {
                let mut conn = pool.get().await.map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Pool Get Connection Error: {}", e)))?;
                conn.del(key).await.map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("`DEL` Command Error: {}", e)))?;
                Ok(())
            })
        })
    }

    fn add(&self, py: Python, value: Vec<&str>) -> PyResult<()> {
        py.allow_threads(move || {
            let pool = self.pool.clone();
            async_std::task::block_on(async move {
                let mut conn = pool.get().await.map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Pool Get Connection Error: {}", e)))?;
                conn.sadd("RS_FINISHED", value).await.map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("`SADD` Command Error: {}", e)))?;
                Ok(())
            })
        })
    }

    fn remove(&self, py: Python,value:Vec<&str>) -> PyResult<()> {
        py.allow_threads(move || {
            let pool = self.pool.clone();
            async_std::task::block_on(async move {
                let mut conn = pool.get().await.map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Pool Get Connection Error: {}", e)))?;
                conn.del(&value).await.map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("`DEL` Command Error: {}", e)))?;
                conn.srem("RS_FINISHED",&value).await.map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("`SREM` Command Error: {}", e)))?;
                Ok(())
            })
        })
    }

    fn members(&self, py: Python) -> PyResult<Vec<String>> {
        py.allow_threads(move || {
            let pool = self.pool.clone();
            async_std::task::block_on(async move {
                let mut conn = pool.get().await.map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Pool Get Connection Error: {}", e)))?;
                let result: Vec<String> = conn.smembers("RS_FINISHED").await.map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("`SMEMBERS` Command Error: {}", e)))?;
                Ok(result)
            })
        })
    }
}

#[pymodule]
fn pyrd(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<RedisPool>()?;
    Ok(())
}


#[cfg(test)]
mod test {
    use serde_json::json;
    use super::*;

    #[test]
    fn t_sorted() {
        let mut data = vec![("1".to_string(), "a".to_string()), ("0".to_string(), "b".to_string()), ("2".to_string(), "c".to_string())];
        println!("{:?}", &data);
        data.sort_by(|a, b| b.0.cmp(&a.0));
        println!("{:?}", &data);
        let slice = &data[..2];
        println!("{:?}", &slice);
    }

    #[test]
    fn t_json() {
        let input = "1.1,2.2,3.3,4.4,5.5,6.6,7.7".to_string();
        let values: Vec<&str> = input.split(',').collect();

        let mut json_obj = serde_json::Map::new();
        for (index, value) in values.iter().enumerate() {
            let key = format!("c{}", index + 1);
            json_obj.insert(key, json!(value));
        }

        let json_string = serde_json::to_string(&json_obj).unwrap();
        println!("{}", json_string);
    }

    #[test]
    fn t_parse() {
        let input = vec![
            ("0".to_string(), "1|2|3|4|5|6;11|22|33|44|55|66;111|222|333|444|555|666".to_string()),
            ("1".to_string(), "a|b|c|d|e|f;aa|bb|cc|dd|ee|ff;aaa|bbb|ccc|ddd|eee|fff".to_string()),
        ];
        let mut a1 = A1::default();
        let json_str = a1.parse(input.as_slice());
        match json_str {
            Ok(str) => println!("{}", str),
            Err(err) => println!("{:?}", err)
        }
    }

    #[test]
    fn t_insert_imgs() {
        let items: Vec<Vec<Vec<&str>>> = vec![
            vec![
                vec!["0.1", "0.2", "0.3", "0.4", "0.5", "0.6"],
                vec!["1.1", "1.2", "1.3", "1.4", "1.5", "1.6"],
            ],
            vec![
                vec!["2.1", "2.2", "2.3", "2.4", "2.5", "2.6"],
                vec!["3.1", "3.2", "3.3", "3.4", "3.5", "3.6"],
            ],
        ];
        let mut insert_vec: Vec<(String, String)> = Vec::new();
        for (index, imgs) in items.iter().enumerate() {
            let mut tmp_vec: Vec<String> = Vec::new();
            for img in imgs.iter() {
                tmp_vec.push(img.join("|"));
            }
            insert_vec.push((index.to_string(), tmp_vec.join(";")));
        }

        println!("{:?}", insert_vec);
    }

    #[test]
    fn t_get_imgs_all() {
        let url = env::var("RS_URL").unwrap();
        let client = Client::open(url).map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(format!("ConnectionManager Error: {}", e))).unwrap();
        let manager = RedisConnectionManager::new(client);
        let pool = mobc::Pool::builder().build(manager);
        async_std::task::block_on(async move {
            let mut conn = pool.get().await.map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Pool Get Connection Error: {}", e))).unwrap();
            let mut result: Vec<(String, String)> = conn.hgetall("f72b6cbb-be73-11ee-a59b-4c77cb4cceca").await.map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("`HGETALL` Command Error: {}", e))).unwrap();
            result.sort_by(|a, b| b.0.cmp(&a.0));
            let mut a1 = A1::default();
            match env::var("RS_LIMIT") {
                Ok(limit) => {
                    let limit = limit.parse::<usize>().unwrap_or(10);
                    let result = &result[..limit];
                    let json_str = a1.parse(result).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Json Parse Error: {}", e))).unwrap();
                    println!("{}", json_str);
                }
                Err(_) => {
                    let json_str = a1.parse(result.as_slice()).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Json Parse Error: {}", e))).unwrap();
                    println!("{}", json_str);
                }
            }
        })
    }
}