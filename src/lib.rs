use pyo3::prelude::*;
use mobc_redis::{mobc,RedisConnectionManager,redis::Client};
use std::env;
use mobc_redis::redis::AsyncCommands;

#[pyclass]
struct RedisPool{
    pool:mobc::Pool<RedisConnectionManager>,
}

#[pymethods]
impl RedisPool{
    #[new]
    fn new(py:Python)->PyResult<Self>{
        py.allow_threads(move||{
            match env::var("RS_URL"){
                Ok(url) =>{
                    let client = Client::open(url).map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(format!("ConnectionManager Error: {}", e)))?;
                    let manager = RedisConnectionManager::new(client);
                    let pool = mobc::Pool::builder().build(manager);
                    Ok(RedisPool{pool})
                }
                Err(_) => {
                    Err(PyErr::new::<pyo3::exceptions::PyException,_>("`RS_URL` system variable not found"))
                }
            }
        })
    }

    fn hmset(&self,py:Python,key:&str,items:Vec<(&str,&str)>) -> PyResult<()>{
        py.allow_threads(move ||{
            let pool = self.pool.clone();
            async_std::task::block_on(async move {
                let mut conn = pool.get().await.map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError,_>(format!("Pool Get Connection Error: {}",e)))?;
                conn.hset_multiple(key,items.as_slice()).await.map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError,_>(format!("`HMSET` Command Error: {}",e)))?;
                Ok(())
            })
        })
    }

    fn hgetall(&self,py:Python,key:&str) -> PyResult<Vec<(String,String)>>{
        py.allow_threads(move ||{
            let pool = self.pool.clone();
            async_std::task::block_on(async move {
                let mut conn = pool.get().await.map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError,_>(format!("Pool Get Connection Error: {}",e)))?;
                let result:Vec<(String,String)> = conn.hgetall(key).await.map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError,_>(format!("`HGETALL` Command Error: {}",e)))?;
                Ok(result)
            })
        })
    }

    fn hdel(&self,py:Python,key:&str) ->PyResult<()>{
        py.allow_threads(move || {
            let pool = self.pool.clone();
            async_std::task::block_on(async move{
                let mut conn = pool.get().await.map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError,_>(format!("Pool Get Connection Error: {}",e)))?;
                conn.del(key).await.map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError,_>(format!("`DEL` Command Error: {}",e)))?;
                Ok(())
            })
        })

    }
}

#[pymodule]
fn pyrd(_py:Python,m:&PyModule)->PyResult<()>{
    m.add_class::<RedisPool>()?;
    Ok(())
}