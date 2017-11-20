from flask import Flask
from logger import KinesisLogger
from flask import request

app = Flask(__name__)
logger = KinesisLogger("datapao-logging")


@app.route("/")
def name_age():
    age = request.args.get('age')
    name = request.args.get('name')
    if name is None:
        logger.info({"name": name, "code": 400})
        return "Wrong request", 400
    logger.info({"age": age, "name": name, "code": 200})
    return "{} is {} years old".format(name, age)


def main():
    app.run(threaded=True)


if __name__ == "__main__":
    main()
