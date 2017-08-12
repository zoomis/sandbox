---
title: Streaml sandbox
description: Interact with a single node streaml platform.
tags:
- heron
- pulsar
- onebox
---


# Running the Sandbox Image

### Clone the repo
```bash
git clone https://github.com/streamlio/sandbox.git
```

### Build the Docker Image
```bash
docker build docker/ -t streamlio-sandbox:latest
docker images
```

### Run a container based on the image
```bash
docker run -id --name streamlio-sandbox \
-p 8000:9000 -p 7889:8889 -p 6650:6650 -p 7080:8080 streamlio-sandbox:latest
docker ps
```

# Examining Running Toplogy

Go to host http://localhost:8889/

# Sending Input Events

```bash
python producer.py
```

# Consuming Output Events

```bash
python consumer.py
```

# Shutdown container

```bash
docker kill streamlio-sandbox
```


