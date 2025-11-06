# mqttsn12-client
Java Client implementation of the Protocol MQTT-SN 1.2 (https://mqtt.org/mqtt-specification/ or https://groups.oasis-open.org/higherlogic/ws/public/document?document_id=66091).

The library was tested with EMQX (https://www.emqx.com/en) and Hive MQ Edge (https://www.hivemq.com/products/hivemq-edge/).

Below you can find the list of the Message Type implemented:

|MsgType|Field|Status|Note|
|-|-|-|-|
|0x00|ADVERTISE|NA||
|0x01|SEARCHGW|Implemented||
|0x02|GWINFO|Implemented||
|0x03|reserved|NA||
|0x04|CONNECT|Implemented||
|0x05|CONNACK|Implemented||
|0x06|WILLTOPICREQ|Implemented||
|0x07|WILLTOPIC|Implemented||
|0x08|WILLMSGREQ|Implemented||
|0x09|WILLMSG|Implemented||
|0x0A|REGISTER|Implemented||
|0x0B|REGACK|Implemented||
|0x0C|PUBLISH|Implemented||
|0x0D|PUBACK|Implemented||
|0x0E|PUBCOMP|Implemented||
|0x0F|PUBREC|Implemented||
|0x10|PUBREL|Implemented||
|0x11|reserved|NA||
|0x12|SUBSCRIBE|Implemented||
|0x13|SUBACK|Implemented||
|0x14|UNSUBSCRIBE|Implemented||
|0x15|UNSUBACK|Implemented||
|0x16|PINGREQ|Implemented||
|0x17|PINGRESP|Implemented||
|0x18|DISCONNECT|Implemented||
|0x19|reserved|NA||
|0x1A|WILLTOPICUPD|Implemented||
|0x1B|WILLTOPICRESP|Implemented||
|0x1C|WILLMSGUPD|Implemented||
|0x1D|WILLMSGRESP|Implemented||
|0x1E-0xFD|reserved|NA||
|0xFE|Encapsulated message|NA||
|0xFF|reserved|NA||
