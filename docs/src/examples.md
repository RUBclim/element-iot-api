# Code examples

A few examples how to use the library to retrieve data from the API

## Data from readings for devices in a folder

This examples first retrieves all addresses in a folder using
{meth}`element.ElementApi.get_device_addresses` and then iterates over the address to
retrieve readings from each device using {meth}`element.ElementApi.get_readings`,
subsequently combining them to one dataframe. Also corresponding metadata is gathered by
calling {meth}`element.ElementApi.get_device` and added to the
{class}`pandas.DataFrame`.

```python
import os
from datetime import datetime
from typing import Any

import pandas as pd

from element import ElementApi


def _add_metadata(
        df: pd.DataFrame,
        api: ElementApi,
        device_name: str,
) -> pd.DataFrame:
    metadata: dict[str, Any] = {}
    device_info = api.get_device(address=device_name)
    location = device_info['body']['location']
    if location:
        metadata['lon'], metadata['lat'] = location['coordinates']
    else:
        metadata['lon'], metadata['lat'] = float('nan'), float('nan')

    metadata['name'] = device_info['body']['name']
    metadata['bemerkung'] = device_info['body']['fields']['gerateinformation']['bemerkung']  # noqa: E501
    metadata['strasse'] = device_info['body']['fields']['gerateinformation']['strasse']  # noqa: E501
    df_meta = pd.DataFrame(metadata, index=[device_name])
    return pd.merge(
        left=df.reset_index(),
        right=df_meta,
        how='left',
        on='name',
    )


def main() -> int:
    api = ElementApi(
        api_location='https://dew21.element-iot.com/api/v1/',
        api_key=os.environ['API_KEY'],
    )
    START = datetime(2024, 8, 9, 16, 0)
    # SHT35 devices
    print('getting SHT35 data...')
    sht35_addresses = api.get_device_addresses(
        'stadt-dortmund-klimasensoren-aktiv-sht35',
    )
    sht35_df_list = []
    for idx, addr in enumerate(sht35_addresses):
        print(f'{idx + 1}/{len(sht35_addresses)}')
        readings_df = api.get_readings(
            device_name=addr,
            start=START,
            as_dataframe=True,
        )
        readings_df['name'] = addr
        readings_df = _add_metadata(
            df=readings_df,
            api=api,
            device_name=addr,
        ).set_index('measured_at')
        sht35_df_list.append(readings_df)

    df_sht35 = pd.concat(sht35_df_list)
    df_sht35.to_csv('t.csv')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
```

## Data from packets in a folder

This examples retrieves data from all three types of stations (SHT35, BLG, ATM41) and
adds some metadata to the retrieved data an finally saves it to a csv file.

First raw, unparsed data is requested as packets using
{meth}`element.ElementApi.get_packets`. The data is the parsed, using the custom
`_parse` function which uses a decoder e.g. {meth}`element.parsers.decode_SHT35` to
retrieve data from the packet. Finally metadata is added with a custom `_add_metadata`
function by using {meth}`element.ElementApi.get_device` and the `decentlab_id` is
converted to an address by using {meth}`element.ElementApi.address_from_decentlab_id`.

```python
import os
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime
from typing import Any

import pandas as pd

from element import decode_ATM41
from element import decode_BLG
from element import decode_STH35
from element import ElementApi
from element.element_api import ApiReturn
from element.element_api import Packet
from element.parsers import ATM41Measurement
from element.parsers import BLGMeasurement
from element.parsers import SHT35Measurement

RType = SHT35Measurement | BLGMeasurement | ATM41Measurement


def _parse(
        packets: ApiReturn[list[Packet]],
        decoder: Callable[[bytes, bool, int], RType],
) -> pd.DataFrame:
    data: dict[str, Any] = defaultdict(list)
    for p in packets['body']:
        date = datetime.fromisoformat(p['inserted_at'])
        data['date'].append(date)
        payload = p['payload']
        if payload is not None:
            vals = decoder(payload, True, 2)
            for k, v in vals.items():
                if isinstance(v, dict):
                    data[k].append(v['value'])
                else:
                    data[k].append(v)

    return pd.DataFrame(data).set_index('date').sort_index()


def _add_metadata(
        df: pd.DataFrame,
        api: ElementApi,
        folder: str,
) -> pd.DataFrame:
    df_meta_list = []
    for device_id in df['Device ID'].dropna().unique():
        metadata: dict[str, Any] = {}
        device_info = api.get_device(
            api.address_from_decentlab_id(
                device_id,
                folder=folder,
            ),
        )
        location = device_info['body']['location']
        if location:
            metadata['lon'], metadata['lat'] = location['coordinates']
        else:
            metadata['lon'], metadata['lat'] = float('nan'), float('nan')

        metadata['name'] = device_info['body']['name']
        metadata['bemerkung'] = device_info['body']['fields']['gerateinformation']['bemerkung']  # noqa: E501
        metadata['strasse'] = device_info['body']['fields']['gerateinformation']['strasse']  # noqa: E501
        df_meta_list.append(pd.DataFrame(metadata, index=[device_id]))

    df_meta = pd.concat(df_meta_list)
    df_meta.index.name = 'Device ID'
    return pd.merge(
        left=df.reset_index(),
        right=df_meta,
        how='left',
        on='Device ID',
    )


def main() -> int:
    api = ElementApi(
        api_location='https://dew21.element-iot.com/api/v1/',
        api_key=os.environ['API_KEY'],
    )
    START = datetime(2024, 8, 9, 16, 0)
    print('getting SHT35 data...')
    packets_ta = api.get_packets(
        folder='stadt-dortmund-klimasensoren-aktiv-sht35',
        packet_type='up',
        start=START,
    )
    df_ta = _parse(packets=packets_ta, decoder=decode_STH35)
    df_ta = _add_metadata(
        df=df_ta,
        api=api,
        folder='stadt-dortmund-klimasensoren-aktiv-sht35',
    ).set_index('date')
    df_ta.to_csv('temp_data.csv')

    print('getting BG data...')
    packets_bg = api.get_packets(
        folder='stadt-dortmund-klimasensoren-aktiv-blackglobe',
        packet_type='up',
        start=START,
    )
    df_bg = _parse(packets=packets_bg, decoder=decode_BLG)
    df_bg = _add_metadata(
        df=df_bg,
        api=api,
        folder='stadt-dortmund-klimasensoren-aktiv-blackglobe',
    )
    df_bg.to_csv('bg_data.csv')

    print('getting ATM41 data...')
    packets_atm41 = api.get_packets(
        folder='stadt-dortmund-klimasensoren-aktiv-atm41',
        packet_type='up',
        start=START,
    )
    df_atm41 = _parse(packets=packets_atm41, decoder=decode_ATM41)
    df_atm41 = _add_metadata(
        df=df_atm41,
        api=api,
        folder='stadt-dortmund-klimasensoren-aktiv-atm41',
    )
    df_atm41.to_csv('atm41_data.csv')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
```
