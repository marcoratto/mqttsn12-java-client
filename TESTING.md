# Just for testing

## Docker Desktop

Follow the instruction from [https://www.docker.com/get-started/]

## Broker MQTT EMQX

Install EMQX:
```
docker run -d --name emqx -p 1883:1883 -p 8083:8083 -p 8084:8084 -p 8883:8883 -p 18083:18083 -p 2442:2442/udp emqx/emqx:latest
```

## Topics

|Topic Name|Topic Type|Note|
|-|-|-|
|mqttsn/status| Normal | |
|mqttsn/sample| Normal | |
|mqttsn/test/predefined_pub| Predefined | Assign value "1"|
|mqttsn/test/predefined_sub| Predefined | Assign value "2"|
