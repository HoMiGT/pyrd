import pyrd

anti_fake_id = "f72b6cbb-be73-11ee-a59b-4c77cb4cceca"

pool = pyrd.RedisPool()
print(dir(pool))


def insert():
    pool.insert(anti_fake_id, [("dt_begin", "2024-01-29 15:07:00")])
    pool.insert(anti_fake_id, [("dt_push", "2024-01-29 15:07:05"), ("dt_end", "2024-01-29 15:07:10")])
    pool.insert(anti_fake_id, [("dt_do", "2024-01-29 15:07:13"), ("dt_done", "2024-01-29 15:07:18")])
    pool.insert(anti_fake_id, [("dt_end", "2024-01-29 15:07:00")])
    pool.insert(anti_fake_id, [("label_code", "41020312220000120")])
    pool.insert(anti_fake_id, [("verify_cfg", "{\"a\":\"b\"}")])
    pool.insert(anti_fake_id, [("service_type", "0"), ("service_version", "2")])
    pool.insert(anti_fake_id, [("result", "1"), ("img_num", "30")])


# insert()


def get_all():
    result = pool.get_all(anti_fake_id)
    print(result)


get_all()


def insert_imgs():
    imgs_params = [
        [
            ["0.1", "0.2", "0.3", "0.4", "0.5", "0.6"],
            ["1.1", "1.2", "1.3", "1.4", "1.5", "1.6"],
        ],
        [
            ["2.1", "2.2", "2.3", "2.4", "2.5", "2.6"],
            ["3.1", "3.2", "3.3", "3.4", "3.5", "3.6"],
        ],
    ]
    pool.insert_imgs(anti_fake_id, imgs_params)


# insert_imgs()


def get_all_imgs():
    res = pool.get_all_imgs(anti_fake_id)
    print(type(res))
    print(res)


get_all_imgs()


def add_member():
    pool.add(["k1", "k2"])
    pool.add(["k1", "k2"])
    pool.add(["k3"])


add_member()


def get_members():
    res = pool.members()
    print("members:{}".format(res))
    return res

get_members()


def delete_member():
    res = get_members()
    pool.remove(res)


delete_member()

get_members()
