[![ci](https://github.com/RUBclim/element-iot-api/actions/workflows/ci.yml/badge.svg)](https://github.com/RUBclim/element-iot-api/actions/workflows/ci.yml)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/RUBclim/element-iot-api/main.svg)](https://results.pre-commit.ci/latest/github/RUBclim/element-iot-api/main)
[![deploy docs to gh-page](https://github.com/RUBclim/element-iot-api/actions/workflows/pages.yaml/badge.svg)](https://github.com/RUBclim/element-iot-api/actions/workflows/pages.yaml)

# element-iot-api

**A separate sphinx documentation exists and can be found here:
[Docs](https://rubclim.github.io/element-iot-api)**

## Installation

via pip

```bash
pip install element-iot-api
```

via https

```bash
pip install git+https://github.com/RUBclim/element-iot-api
```

via ssh

```bash
pip install git+ssh://git@github.com/RUBclim/element-iot-api
```

## Quick start

To get started interacting with the API, you will need an API key. Do not store the API
key in your code and especially do not commit the API key. Provide the API key via
environment variables.

Get started by creating an instance of `element.ElementApi`.

```python
import os
from element import ElementApi

api = ElementApi(
    api_location='https://dew21.element-iot.com/api/v1/',
    api_key=os.environ['API_KEY'],
)
```

If you already know the address of an device, you can get data as a `pandas.DataFrame`
as easy as:

```python
df = api.get_readings(
    device_name='DEC005304',
    start=datetime(2024, 8, 15, 9, 0),
    end=datetime(2024, 8, 15, 10, 0),
    as_dataframe=True,
)
```

|           measured_at            | air_humidity | air_temperature | battery_voltage | device_id | protocol_version |
| :------------------------------: | :----------: | :-------------: | :-------------: | :-------: | :--------------: |
| 2024-08-15 09:00:43.730454+00:00 |   71.4382    |     21.1841     |      3.073      |   21668   |        2         |
|               ...                |     ...      |       ...       |       ...       |    ...    |       ...        |
| 2024-08-15 09:55:42.164904+00:00 |   63.4684    |     22.6661     |      3.073      |   21668   |        2         |

### Converting between identifiers

There are multiple identifiers per station on the one hand the `decentlab_id` (e.g.
`21668`) which is the serial number, an integer and the hexadecimal mac address (e.g.
`DEC005304`).

#### decentlab_id &rarr; address

You can easily get the `decentlab_id` from the address using
`element.ElementApi.decentlab_id_from_address`.

```python
api.decentlab_id_from_address('DEC0054A4')
```

This will return `21668`. The result will be cached, so no request is made, the next
time you call it.

#### address &rarr; decentlab_id

Converting from the address to the `decentlab_id` is no so easy, since there is no
direct mapping possible in the API. So the first time calling this may be slow.
Afterwards it will also be cached in the `element.ElementApi` instance.

This time we need to specify a folder when calling
`element.ElementApi.address_from_decentlab_id`.

```python
api.address_from_decentlab_id(
    decentlab_id=21668,
    folder='stadt-dortmund-klimasensoren-aktiv-sht35'
)
```

This will return `DEC0054A4`. The result will be cached, so no request is made, the next
time you call it.

### listing the folders

The Element system is structured and organized based on folders where sensors are stored
in. You can get a list of all available folders by calling
`element.ElementApi.get_folders`:

```python
api.get_folders()
```

...which will return the raw api request. Or if you're just interested in the
folder-slugs, which are combined identifiers for each folder, you can use
`element.ElementApi.get_folder_slugs`:

```python
api.get_folder_slugs()
```

which will return a list of of folder-slugs:

```console
['stadt-dortmund-klimasensoren-aktiv-sht35', ..., 'stadt-dortmund-klimasensoren-aktiv-atm41']

```

### Listing devices

Each folder contains devices which you can list using `element.ElementApi.get_devices`.
This will return the raw API response you can use to extract information.

```python
api.get_devices()
```

If you are just interested in the device addresses, e.g. to retrieve data from all
devices in a folder, you can use `element.ElementApi.get_device_addresses`.

```python
api.get_device_addresses(folder='stadt-dortmund-klimasensoren-aktiv-sht35')
```

...which will return a list of strings which correspon to the device addresses of all
devices in the folder

```console
['DEC0054A6', 'DEC0054B0', ..., 'DEC0054C6']
```

### Getting detailed device information

Based on the address e.g. `DEC0054C6`, we can get detailed information from one station
using `element.ElementApi.get_device`.

```python
api.get_device('DEC0054C6')
```

This returns the raw API response where you can extract data from.

### Getting data

#### As readings

As described [above](#quick-start), you can get data for one station using
`element.ElementApi.get_readings`. `as_dataframe` will determine whether to return the
raw API response or already a `pandas.DataFrame`.

```python
data = api.get_readings(
    device_name='DEC0054C6',
    start=datetime(2024, 8, 1, 0, 0),
    end=datetime(2024, 8, 10, 0, 0),
    as_dataframe=True,
)
```

Additionally, you may specify the following keyword-arguments:

- **sort**: how to sort the data (by either `measured_at` or `inserted_at`)
- **sort_direction**: either `asc` or `desc`
- **limit**: how many value to fetch per paginated request
- **max_page**: how many pages of pagination to get maximum to avoid infinite pagination

#### From packets

If the original data was not parsed or incorrectly parsed, you can get the data directly
from the unparsed packets. The raw packets can be retrieved using
`element.ElementApi.get_packets` by either specifying the `device_name` **or** the
`folder` (to get data for all stations in this folder). If you want to just get the
measurements, you need to specify `packet_type='up'`, for uplink packages.

```python
packets = api.get_packets(
    folder='stadt-dortmund-klimasensoren-aktiv-sht35',
    packet_type='up',
    start=datetime(2024, 8, 1, 0, 0),
    end=datetime(2024, 8, 10, 0, 0),
)
```

This will return the raw package data, where you will need to extract the message from
und subsequently parse it.

### Parsing packets

Code for parsing the packets is provided by decentlab and was vendored into this repo.

This packages provides functions for parsing different sensor types.

#### BLG

Use `element.parsers.decode_BLG` to decode a message from the BLG sensor.

```python
data = decode_BLG(msg=b'0254970003498800830BF7', hex=True)
```

This will return a dictionary similar to this:

```python
{
   'Battery voltage': {
      'unit': 'V',
      'value': 3.063,
   },
   'Device ID': 21655,
   'Protocol version': 2,
   'Temperature': {
      'unit': '°C',
      'value': 47.728822273125274,
   },
   'Thermistor resistance': {
      'unit': 'Ω',
      'value': 36877.08418433659,
   },
   'Voltage ratio': {
      'unit': None,
      'value': 0.012840747833251953,
   },
}
```

#### SHT35

Use `element.parsers.decode_STH35` to decode a message from the SHT35 sensor.

```python
data = decode_STH35(msg=b'0254A60003783F596E0C17', hex=True)
```

#### ATM41

Use `element.parsers.decode_ATM41` to decode a message from the SHT35 sensor.

```python
data = decode_ATM41(
    msg=b'02530400038283800080008000803488CD8076815C80CBA708816D817D80197FF680007FDB7FDB0AAE',
    hex=True,
)
```
