# pyrd
python调用rust的redis

# 编译成python的whl
```shell
maturin build -r
pip install 生成.whl 
```

# 直接安装
```shell
maturin develop -r
```
 

# 系统变量
```
RS_URL: 连接redis的url,必须
RS_IS_RECORD： 是否记录json格式的信息,非必须,不配置为false
RS_LIMIT: 倒叙记录图片信息的数量,非必须,不配置则全部记录
RS_SPLIT_1: 字符串分割符号,非必须,默认为 ";"  字符串形如 c1|c2|...;c1|c2|...
RS_SPLIT_2: 字符串分割符号,非必须,默认为 "|"  字符串形如 c1|c2|...;c1|c2|...
```


