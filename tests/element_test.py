import io
import json
from datetime import datetime
from datetime import timezone
from typing import Any
from unittest.mock import call
from unittest.mock import MagicMock
from unittest.mock import patch
from urllib.error import HTTPError

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from element import ElementApi


@pytest.fixture
def api() -> ElementApi:
    return ElementApi(
        api_location='https://testing.element-iot.com/api/v1/',
        api_key='123456789ABCDEFG',
    )


def _resp(path: str) -> io.BytesIO:
    with open(path, 'rb') as f:
        return io.BytesIO(f.read())


def test_api_repr_key_is_hidden(api: ElementApi) -> None:
    assert repr(api) == (
        "ElementApi(api_location='https://testing.element-iot.com/api/v1', "
        'api_key=*************EFG)'
    )


@pytest.mark.parametrize(
    'obj',
    (
        None,
        'api',
        ElementApi(
            api_location='https://test.element-iot.com/api/v1/',
            api_key='123456789ABCDEFG',
        ),
        ElementApi(
            api_location='https://testing.element-iot.com/api/v1/',
            api_key='123456789ABCDEF0',
        ),
    ),
)
def test_equality_not_equal(api: ElementApi, obj: Any) -> None:
    eq = api == obj
    assert eq is False


def test_equality_is_equal(api: ElementApi) -> None:
    eq = api == ElementApi(
        api_location='https://testing.element-iot.com/api/v1/',
        api_key='123456789ABCDEFG',
    )
    assert eq is True


@patch(
    'urllib.request.urlopen',
    side_effect=[
        io.BytesIO(b'{"body": {}, "retrieve_after_id": "sth"}'),
        io.BytesIO(b'{"body": {}}'),
    ],
)
def test_make_req_body_not_array_but_paginated(
        m: MagicMock,
        api: ElementApi,
) -> None:
    folder = 'stadt-dortmund-klimasensoren-aktiv-sht35'
    with pytest.raises(TypeError) as exc_info:
        api.get_devices(folder=folder)

    msg, = exc_info.value.args
    assert msg == 'cannot handle pagination when `body` is not an array'
    assert m.call_count == 2


@patch(
    'urllib.request.urlopen',
    return_value=_resp('testing/api_resp/device.json'),
)
def test_decentlab_id_from_address_not_cached(
        m: MagicMock,
        api: ElementApi,
) -> None:
    folder = 'stadt-dortmund-klimasensoren-aktiv-sht35'
    # check we have no cache
    assert api._id_to_address_mapping == {}
    decentlab_address = api.decentlab_id_from_address(
        address='DEC0054B0',
        folder=folder,
    )
    assert decentlab_address == 21680
    m.assert_called_once_with(
        'https://testing.element-iot.com/api/v1/devices/dec0054b0?&auth=123456789ABCDEFG',  # noqa: E501
        timeout=5,
    )
    # check that the cache is populated
    assert api._id_to_address_mapping[folder][21680] == 'DEC0054B0'


@patch(
    'urllib.request.urlopen',
    return_value=_resp('testing/api_resp/device.json'),
)
def test_decentlab_id_from_address_folder_unknown(
        m: MagicMock,
        api: ElementApi,
) -> None:
    # check we have no cache
    assert api._id_to_address_mapping == {}
    decentlab_address = api.decentlab_id_from_address(address='DEC0054B0')
    assert decentlab_address == 21680
    m.assert_called_once_with(
        'https://testing.element-iot.com/api/v1/devices/dec0054b0?&auth=123456789ABCDEFG',  # noqa: E501
        timeout=5,
    )
    # check that the cache is now populated
    folder = 'stadt-dortmund-klimasensoren-aktiv-sht35'
    assert api._id_to_address_mapping[folder][21680] == 'DEC0054B0'


@patch(
    'urllib.request.urlopen',
    return_value=_resp('testing/api_resp/device.json'),
)
def test_decentlab_id_from_address_in_cached(
        m: MagicMock,
        api: ElementApi,
) -> None:
    folder = 'stadt-dortmund-klimasensoren-aktiv-sht35'
    # manually add it to the cache
    api._id_to_address_mapping[folder][21680] = 'DEC0054B0'

    decentlab_address = api.decentlab_id_from_address(
        address='DEC0054B0',
        folder=folder,
    )
    assert decentlab_address == 21680
    m.assert_not_called()
    assert api._id_to_address_mapping == {folder: {21680: 'DEC0054B0'}}


def test_address_to_id_mapping_inverse(api: ElementApi) -> None:
    api._id_to_address_mapping['folder_a'][1] = 'DEC0054B0'
    api._id_to_address_mapping['folder_a'][2] = 'DEC0054B1'
    api._id_to_address_mapping['folder_b'][1] = 'DEC0054B2'
    api._id_to_address_mapping['folder_b'][2] = 'DEC0054B3'

    assert api._address_to_id_mapping == {
        'folder_a': {
            'DEC0054B0': 1,
            'DEC0054B1': 2,
        },
        'folder_b': {
            'DEC0054B2': 1,
            'DEC0054B3': 2,
        },
    }


@patch(
    'urllib.request.urlopen',
    side_effect=[
        # 1st call for devices with pagination
        _resp('testing/api_resp/devices_1.json'),
        # 2nd call for devices without pagination
        _resp('testing/api_resp/devices_2.json'),
        # readings that are not the stations
        _resp('testing/api_resp/readings_DEC0054A6.json'),
        # # readings that are the station
        _resp('testing/api_resp/readings_DEC0054B0.json'),
    ],
)
def test_address_from_decentlab_id_not_cached(
        m: MagicMock,
        api: ElementApi,
) -> None:
    # no cache
    assert api._id_to_address_mapping == {}
    address = api.address_from_decentlab_id(
        decentlab_id=21680,
        folder='stadt-dortmund-klimasensoren-aktiv-sht35',
    )
    assert address == 'DEC0054B0'
    # cache is now populated also with the station that we got additionally
    assert api._id_to_address_mapping == {
        'stadt-dortmund-klimasensoren-aktiv-sht35': {
            21670: 'DEC0054A6',
            21680: 'DEC0054B0',
        },
    }
    # 1st devices call that provides us with a `retrieve_after`
    assert m.call_args_list[0] == call(
        'https://testing.element-iot.com/api/v1/tags/stadt-dortmund-klimasensoren-aktiv-sht35/devices?&auth=123456789ABCDEFG',  # noqa: E501
        timeout=5,
    )
    # now the retireve after is added
    assert m.call_args_list[1] == call(
        'https://testing.element-iot.com/api/v1/tags/stadt-dortmund-klimasensoren-aktiv-sht35/devices?&auth=123456789ABCDEFG&retrieve_after=435f6eb8-5d22-4b8c-bdce-1830b7438539',  # noqa: E501
        timeout=5,
    )
    # 1st readings call which is not the station we want
    assert m.call_args_list[2] == call(
        'https://testing.element-iot.com/api/v1/devices/by-name/DEC0054A6/readings?&auth=123456789ABCDEFG&sort=measured_at&sort_direction=asc&limit=1',  # noqa: E501
        timeout=5,
    )
    # 2nd readings call which is the station we want
    assert m.call_args_list[3] == call(
        'https://testing.element-iot.com/api/v1/devices/by-name/DEC0054B0/readings?&auth=123456789ABCDEFG&sort=measured_at&sort_direction=asc&limit=1',  # noqa: E501
        timeout=5,
    )


@patch('urllib.request.urlopen')
def test_address_from_decentlab_id_is_cached(
        m: MagicMock,
        api: ElementApi,
) -> None:
    folder = 'stadt-dortmund-klimasensoren-aktiv-sht35'
    # manually add it to the cache
    api._id_to_address_mapping[folder][21680] = 'DEC0054B0'

    address = api.address_from_decentlab_id(
        decentlab_id=21680,
        folder='stadt-dortmund-klimasensoren-aktiv-sht35',
    )
    assert address == 'DEC0054B0'

    m.assert_not_called()
    assert api._id_to_address_mapping == {folder: {21680: 'DEC0054B0'}}


@patch(
    'urllib.request.urlopen',
    side_effect=[
        # 1st call for devices with pagination
        _resp('testing/api_resp/devices_short.json'),
        # readings that are not the stations
        _resp('testing/api_resp/readings_DEC0054A6.json'),
        # # readings that are also not the station
        _resp('testing/api_resp/readings_DEC0054B0.json'),
    ],
)
def test_address_from_decentlab_id_unable_to_fine_station(
        m: MagicMock,
        api: ElementApi,
) -> None:
    with pytest.raises(ValueError) as exc_info:
        api.address_from_decentlab_id(
            decentlab_id=1233456789,
            folder='stadt-dortmund-klimasensoren-aktiv-sht35',
        )

    assert m.call_count == 3
    msg, = exc_info.value.args
    assert msg == 'unable to find address for station: 1233456789'


@patch(
    'urllib.request.urlopen',
    side_effect=[
        # 1st call for devices with pagination
        _resp('testing/api_resp/folders_1.json'),
        # readings that are not the stations
        _resp('testing/api_resp/folders_2.json'),
    ],
)
def test_get_folder_slugs(m: MagicMock, api: ElementApi) -> None:
    folders = api.get_folder_slugs()
    assert folders == [
        'dew21-service-button-lager',
        'stadt-dortmund-erlebnisroute-lager',
        'stadt-dortmund-klimasensoren-lager-sht35',
        'stadt-dortmund-klimasensoren-lager-blackglobe',
        'stadt-dortmund-klimasensoren-lager-atm41',
        'stadt-dortmund-klimasensoren-lager',
        'stadt-dortmund-klimasensoren-aktiv-sht35',
        'stadt-dortmund-klimasensoren-aktiv-blackglobe',
        'stadt-dortmund-klimasensoren-aktiv-atm41',
        'fabido-inaktiv',
        'stadt-dortmund-erlebnisroute-stoerung',
        'stadt-dortmund-erlebnisroute-inaktiv',
        'stadt-dortmund-erlebnisroute-aktiv',
        'stadt-dortmund-klimasensoren-inaktiv-atm41',
        'stadt-dortmund-klimasensoren-inaktiv-sht35',
        'stadt-dortmund-klimasensoren-inaktiv-blackglobe',
        'stadt-dortmund-parksensoren-stoerung',
        'stadt-dortmund-parksensoren-inaktiv',
        'stadt-dortmund-parksensoren-aktiv',
        'stadt-dortmund-klimasensoren-stoerung',
    ]
    assert m.call_count == 2
    # request is paginated
    assert m.call_args_list[0] == call(
        'https://testing.element-iot.com/api/v1/tags?&auth=123456789ABCDEFG',
        timeout=5,
    )
    assert m.call_args_list[1] == call(
        'https://testing.element-iot.com/api/v1/tags?&auth=123456789ABCDEFG&retrieve_after=0a2eacc2-eb3c-4b44-a9c8-cff9411747ac',  # noqa: E501
        timeout=5,
    )


@patch(
    'urllib.request.urlopen',
    side_effect=[_resp('testing/api_resp/devices_short.json')],
)
def test_get_device_addresses(m: MagicMock, api: ElementApi) -> None:
    folder = 'stadt-dortmund-klimasensoren-aktiv-sht35'
    addresses = api.get_device_addresses(folder=folder)
    assert addresses == ['DEC0054A6', 'DEC0054B0']


@patch(
    'urllib.request.urlopen',
    side_effect=[_resp('testing/api_resp/readings_DEC0054A6_short.json')],
)
def test_get_readings_as_dataframe(m: MagicMock, api: ElementApi) -> None:
    data = api.get_readings(
        device_name='DEC0054A6',
        sort='measured_at',
        sort_direction='asc',
        start=datetime(2024, 8, 13, 13, 5, tzinfo=timezone.utc),
        end=datetime(2024, 8, 13, 13, 15),
        limit=100,
        max_pages=None,
        as_dataframe=True,
    )
    expected_df = pd.DataFrame(
        {
            'air_humidity': [34.934005, 38.171969],
            'air_temperature': [37.200732, 35.350195],
            'battery_voltage': [3.095, 3.095],
            'device_id': [21670, 21670],
            'protocol_version': [2, 2],
        },
        index=pd.DatetimeIndex(
            [
                '2024-08-13 13:06:03.622052+00:00',
                '2024-08-13 13:11:04.070758+00:00',
            ],
        ),
    )
    expected_df.index.name = 'measured_at'
    assert_frame_equal(left=data, right=expected_df)
    assert m.call_count == 1
    assert m.call_args_list[0] == call(
        'https://testing.element-iot.com/api/v1/devices/by-name/DEC0054A6/readings?&auth=123456789ABCDEFG&sort=measured_at&sort_direction=asc&limit=100&after=2024-08-13T13:05:00Z&before=2024-08-13T13:15:00',  # noqa: E501
        timeout=5,
    )


@patch(
    'urllib.request.urlopen',
    side_effect=[_resp('testing/api_resp/empty_resp.json')],
)
def test_get_readings_as_dataframe_not_data_for_device(
        m: MagicMock,
        api: ElementApi,
        capsys: pytest.LogCaptureFixture,
) -> None:
    data = api.get_readings(
        device_name='DEC0054A6',
        sort='measured_at',
        sort_direction='asc',
        start=datetime(2024, 8, 15, 13, 5),
        end=datetime(2024, 8, 15, 13, 15),
        limit=100,
        max_pages=None,
        as_dataframe=True,
    )
    out, _ = capsys.readouterr()
    assert out == "no data for 'DEC0054A6'\n"
    assert_frame_equal(left=pd.DataFrame(), right=data)


@patch(
    'urllib.request.urlopen',
    side_effect=[_resp('testing/api_resp/readings_DEC0054A6_short.json')],
)
def test_get_readings_raw_format(m: MagicMock, api: ElementApi) -> None:
    raw_data = api.get_readings(
        device_name='DEC0054A6',
        sort='measured_at',
        sort_direction='asc',
        start=datetime(2024, 8, 13, 13, 5),
        end=datetime(2024, 8, 13, 13, 15),
        limit=100,
        max_pages=None,
        as_dataframe=False,
    )
    assert m.call_count == 1
    assert m.call_args_list[0] == call(
        'https://testing.element-iot.com/api/v1/devices/by-name/DEC0054A6/readings?&auth=123456789ABCDEFG&sort=measured_at&sort_direction=asc&limit=100&after=2024-08-13T13:05:00&before=2024-08-13T13:15:00',  # noqa: E501
        timeout=5,
    )
    assert raw_data == json.load(
        _resp('testing/api_resp/readings_DEC0054A6_short.json'),
    )


@patch(
    'urllib.request.urlopen',
    side_effect=[
        _resp('testing/api_resp/readings_DEC0054A6_short_streamed.txt'),
    ],
)
def test_get_readings_streamed(m: MagicMock, api: ElementApi) -> None:
    raw_data = api.get_readings(
        device_name='DEC0054A6',
        sort='measured_at',
        sort_direction='asc',
        start=datetime(2024, 8, 13, 13, 5),
        end=datetime(2024, 8, 13, 13, 15),
        max_pages=None,
        as_dataframe=False,
        stream=True,
    )
    assert m.call_count == 1
    assert m.call_args_list[0] == call(
        'https://testing.element-iot.com/api/v1/devices/by-name/DEC0054A6/readings/stream?&auth=123456789ABCDEFG&sort=measured_at&sort_direction=asc&after=2024-08-13T13:05:00&before=2024-08-13T13:15:00',  # noqa: E501
        timeout=5,
    )
    assert raw_data == json.load(
        _resp('testing/api_resp/readings_DEC0054A6_short.json'),
    )


@patch('urllib.request.urlopen', side_effect=[io.BytesIO(b'')])
def test_get_readings_streamed_no_data(m: MagicMock, api: ElementApi) -> None:
    raw_data = api.get_readings(
        device_name='DEC0054A6',
        sort='measured_at',
        sort_direction='asc',
        start=datetime(2024, 8, 13, 13, 5),
        end=datetime(2024, 8, 13, 13, 15),
        max_pages=None,
        as_dataframe=False,
        stream=True,
    )
    assert m.call_count == 1
    assert m.call_args_list[0] == call(
        'https://testing.element-iot.com/api/v1/devices/by-name/DEC0054A6/readings/stream?&auth=123456789ABCDEFG&sort=measured_at&sort_direction=asc&after=2024-08-13T13:05:00&before=2024-08-13T13:15:00',  # noqa: E501
        timeout=5,
    )
    assert raw_data == {'body': [], 'ok': True, 'status': 200}


@patch(
    'urllib.request.urlopen',
    side_effect=[
        _resp('testing/api_resp/readings_DEC0054A6_short_streamed_error.txt'),
    ],
)
def test_get_readings_timeout_streamed(m: MagicMock, api: ElementApi) -> None:
    with pytest.raises(HTTPError) as exc_info:
        api.get_readings(
            device_name='DEC0054A6',
            sort='measured_at',
            sort_direction='asc',
            start=datetime(2024, 8, 13, 13, 5),
            end=datetime(2024, 8, 13, 13, 15),
            max_pages=None,
            as_dataframe=False,
            stream=True,
            timeout=250,
        )
    assert m.call_count == 1
    assert m.call_args_list[0] == call(
        'https://testing.element-iot.com/api/v1/devices/by-name/DEC0054A6/readings/stream?&auth=123456789ABCDEFG&sort=measured_at&sort_direction=asc&after=2024-08-13T13:05:00&before=2024-08-13T13:15:00&timeout=250',  # noqa: E501
        timeout=5,
    )
    assert exc_info.value.msg == (
        'Database timeout. Try allowing more time by using the timeout query '
        'param (in milliseconds). Current timeout: 250.'
    )


@patch(
    'urllib.request.urlopen',
    side_effect=[_resp('testing/api_resp/packets_by_device_DEC0054A6.json')],
)
def test_get_packets_by_device(m: MagicMock, api: ElementApi) -> None:
    packets = api.get_packets(
        device_name='DEC0054A6',
        packet_type='up',
        start=datetime(2024, 8, 13, 13, 5),
        end=datetime(2024, 8, 13, 13, 15),
        limit=100,
        max_pages=None,
    )
    assert m.call_count == 1
    assert m.call_args_list[0] == call(
        'https://testing.element-iot.com/api/v1/devices/by-name/DEC0054A6/packets?&auth=123456789ABCDEFG&limit=100&packet_type=up&after=2024-08-13T13:05:00&before=2024-08-13T13:15:00',  # noqa: E501
        timeout=5,
    )
    assert packets == json.load(
        _resp('testing/api_resp/packets_by_device_DEC0054A6.json'),
    )


@patch(
    'urllib.request.urlopen',
    side_effect=[_resp('testing/api_resp/packets_by_folder.json')],
)
def test_get_packets_by_folder(m: MagicMock, api: ElementApi) -> None:
    folder = 'stadt-dortmund-klimasensoren-aktiv-sht35'
    packets = api.get_packets(
        folder=folder,
        packet_type='up',
        start=datetime(2024, 8, 13, 13, 5),
        end=datetime(2024, 8, 13, 13, 15),
        limit=100,
        max_pages=None,
    )
    assert m.call_count == 1
    assert m.call_args_list[0] == call(
        'https://testing.element-iot.com/api/v1/tags/stadt-dortmund-klimasensoren-aktiv-sht35/packets?&auth=123456789ABCDEFG&limit=100&packet_type=up&after=2024-08-13T13:05:00&before=2024-08-13T13:15:00',  # noqa: E501
        timeout=5,
    )
    assert packets == json.load(
        _resp('testing/api_resp/packets_by_folder.json'),
    )


@patch(
    'urllib.request.urlopen',
    side_effect=[_resp('testing/api_resp/packets_by_folder_streamed.txt')],
)
def test_get_packets_by_folder_streamed(m: MagicMock, api: ElementApi) -> None:
    folder = 'stadt-dortmund-klimasensoren-aktiv-sht35'
    packets = api.get_packets(
        folder=folder,
        packet_type='up',
        start=datetime(2024, 8, 13, 13, 5),
        end=datetime(2024, 8, 13, 13, 15),
        max_pages=None,
        stream=True,
    )
    assert m.call_count == 1
    assert m.call_args_list[0] == call(
        'https://testing.element-iot.com/api/v1/tags/stadt-dortmund-klimasensoren-aktiv-sht35/packets/stream?&auth=123456789ABCDEFG&packet_type=up&after=2024-08-13T13:05:00&before=2024-08-13T13:15:00',  # noqa: E501
        timeout=5,
    )
    assert packets == json.load(
        _resp('testing/api_resp/packets_by_folder.json'),
    )


@patch('urllib.request.urlopen', side_effect=[io.BytesIO(b'')])
def test_get_packets_by_folder_streamed_no_data(
        m: MagicMock,
        api: ElementApi,
) -> None:
    folder = 'stadt-dortmund-klimasensoren-aktiv-sht35'
    packets = api.get_packets(
        folder=folder,
        packet_type='up',
        start=datetime(2024, 8, 13, 13, 5),
        end=datetime(2024, 8, 13, 13, 15),
        max_pages=None,
        stream=True,
    )
    assert m.call_count == 1
    assert m.call_args_list[0] == call(
        'https://testing.element-iot.com/api/v1/tags/stadt-dortmund-klimasensoren-aktiv-sht35/packets/stream?&auth=123456789ABCDEFG&packet_type=up&after=2024-08-13T13:05:00&before=2024-08-13T13:15:00',  # noqa: E501
        timeout=5,
    )
    assert packets == {'body': [], 'ok': True, 'status': 200}


@patch(
    'urllib.request.urlopen',
    side_effect=[
        _resp('testing/api_resp/packets_by_folder_streamed_error.txt'),
    ],
)
def test_get_packets_by_folder_streamed_error(
        m: MagicMock,
        api: ElementApi,
) -> None:
    folder = 'stadt-dortmund-klimasensoren-aktiv-sht35'
    with pytest.raises(HTTPError) as exc_info:
        api.get_packets(
            folder=folder,
            max_pages=None,
            stream=True,
            timeout=250,
        )
    assert m.call_count == 1
    assert m.call_args_list[0] == call(
        'https://testing.element-iot.com/api/v1/tags/stadt-dortmund-klimasensoren-aktiv-sht35/packets/stream?&auth=123456789ABCDEFG&timeout=250',  # noqa: E501
        timeout=5,
    )
    assert exc_info.value.msg == (
        'Database timeout. Try allowing more time by using the timeout query '
        'param (in milliseconds). Current timeout: 250.'
    )


def test_get_packet_no_device_name_and_folder_set(api: ElementApi) -> None:
    with pytest.raises(TypeError) as exc_info:
        api.get_packets()

    msg, = exc_info.value.args
    assert msg == 'one of device_name or folder needs to be specified'


def test_get_packet_device_name_and_folder_set(api: ElementApi) -> None:
    with pytest.raises(TypeError) as exc_info:
        api.get_packets(device_name='DEC0054A6', folder='some-folder-name')

    msg, = exc_info.value.args
    assert msg == 'only one of device_name or folder must be specified'
