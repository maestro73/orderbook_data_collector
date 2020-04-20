import redis


redis_host = "localhost"
redis_port = 6379
redis_password = ""


def upload(key, value):
    # creates a connection to Redis
    r = redis.Redis(
        host=redis_host,
        port=redis_port,
        password=redis_password)
    try:
        r.ping()
        r.append(key, value) if r.exists(key) else r.set(key, value)
    except Exception as e:
        print(e)
