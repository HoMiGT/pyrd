import pyrd

print(dir(pyrd))

pool = pyrd.RedisPool()
pool.hmset("tmp",[("tmp1","tmp11"),("tmp2","tmp22"),("tmp3","tmp33")])
r = pool.hgetall("tmp")
print(r)