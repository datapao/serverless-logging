![](logo512.png)
# AWS Serverless logging
## Create AWS infrastructure
The following script will ask for the required input and then proceeds to create the necessary resources.
```
python3 bootstrap.py
```

## Send test log messages
To send 1000 log message to the log sytem use the `test.py`.
```
python3 test.py <stream-name> 1000
```

## Logger
We provide an out-of-the-box logger with `logger.py`.
Import the Logger class and use it to log structured messages.
```python
# Initiate the logger with the stream/logging name
logger = KinesisLogger("datapao-logging-3")

# Simple message log
logger.log({ "level": "INFO", "message": "Failed to run batch job" })

# Batch log messages
logger.log_batch([
    { "level": "INFO", "message": "Failed to run batch job" },
    { "level": "DEBUG", "origin": "testing", "timestamp": 1391203123, "sensor": "temp-0002xb" }
])
```

## Contribution
Feel free to send any feedback, PR or unneccesary GIFs.

---

Created with :heart: at <a href="http://datapao.com">![](http://datapao.com/wp-content/themes/datapao/img/header.svg)</a>

