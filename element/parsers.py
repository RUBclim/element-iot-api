"""This Code is partially vendored from:
https://github.com/decentlab/decentlab-decoders Which is licensed under the
MIT License, which is included in this repo
"""
import binascii
import struct
from collections.abc import Callable
from math import log
from typing import cast
from typing import NotRequired
from typing import TypedDict


class _Value(TypedDict):
    name: str
    convert: NotRequired[Callable[[list[float] | tuple[float, ...]], float]]
    unit: NotRequired[str]


class SensorContainer(TypedDict):
    length: int
    values: list[_Value]


class ParamReading(TypedDict):
    unit: str
    value: float


BLGMeasurement = TypedDict(
    'BLGMeasurement',
    {
        'Voltage ratio': ParamReading,
        'Thermistor resistance': ParamReading,
        'Temperature': ParamReading,
        'Battery voltage': ParamReading,
        'Device ID': int,
        'Protocol version': int,
    },
)

SHT35Measurement = TypedDict(
    'SHT35Measurement',
    {
        'Air humidity': ParamReading,
        'Air temperature': ParamReading,
        'Battery voltage': ParamReading,
        'Device ID': int,
        'Protocol version': int,
    },
)

ATM41Measurement = TypedDict(
    'ATM41Measurement',
    {
        'Solar radiation': ParamReading,
        'Precipitation': ParamReading,
        'Lightning strike count': ParamReading,
        'Lightning average distance': ParamReading,
        'Wind speed': ParamReading,
        'Wind direction': ParamReading,
        'Maximum wind speed': ParamReading,
        'Air temperature': ParamReading,
        'Vapor pressure': ParamReading,
        'Atmospheric pressure': ParamReading,
        'Relative humidity': ParamReading,
        'Sensor temperature (internal)': ParamReading,
        'X orientation angle': ParamReading,
        'Y orientation angle': ParamReading,
        'Compass heading': ParamReading,
        'North wind speed': ParamReading,
        'East wind speed': ParamReading,
        'Battery voltage': ParamReading,
        'Device ID': int,
        'Protocol version': int,
    },
)


DECODER_SCHEMAS: dict[str, list[SensorContainer]] = {
    'BLG': [
        {
            'length': 2,
            'values': [
                {
                    'name': 'Voltage ratio',
                    'convert': lambda x: ((x[0] + x[1]*65536) / 8388608 - 1) / 2,  # noqa: E501
                },
                {
                    'name': 'Thermistor resistance',
                    'convert': lambda x: (
                        1000 / (((x[0] + x[1]*65536) / 8388608 - 1) / 2) - 41000  # noqa: E501
                    ),
                    'unit': 'Ω',
                },
                {
                    'name': 'Temperature',
                    'convert': lambda x: (
                        1 / (0.0008271111 + 0.000208802 * log(1000 / (((x[0] + x[1]*65536) / 8388608 - 1) / 2) - 41000) + 0.000000080592 * pow(log(1000 / (((x[0] + x[1]*65536) / 8388608 - 1) / 2) - 41000), 3))  # noqa: E501
                    ) - 273.15,
                    'unit': '°C',
                },
            ],
        },
        {
            'length': 1,
            'values': [{
                'name': 'Battery voltage',
                'convert': lambda x: x[0] / 1000,
                'unit': 'V',
            }],
        },
    ],
    'SHT35': [
        {
            'length': 2,
            'values': [
                {
                    'name': 'Air temperature',
                    'convert': lambda x: 175 * x[0] / 65535 - 45,
                    'unit': '°C',
                },
                {
                    'name': 'Air humidity',
                    'convert': lambda x: 100 * x[1] / 65535,
                    'unit': '%',
                },
            ],
        },
        {
            'length': 1,
            'values': [{
                'name': 'Battery voltage',
                'convert': lambda x: x[0] / 1000,
                'unit': 'V',
            }],
        },
    ],
    'ATM41': [
        {
            'length': 17,
            'values': [
                {
                    'name': 'Solar radiation',
                    'convert': lambda x: x[0] - 32768,
                    'unit': 'W⋅m⁻²',
                },
                {
                    'name': 'Precipitation',
                    'convert': lambda x: (x[1] - 32768) / 1000,
                    'unit': 'mm',
                },
                {
                    'name': 'Lightning strike count',
                    'convert': lambda x: x[2] - 32768,
                },
                {
                    'name': 'Lightning average distance',
                    'convert': lambda x: x[3] - 32768,
                    'unit': 'km',
                },
                {
                    'name': 'Wind speed',
                    'convert': lambda x: (x[4] - 32768) / 100,
                    'unit': 'm⋅s⁻¹',
                },
                {
                    'name': 'Wind direction',
                    'convert': lambda x: (x[5] - 32768) / 10,
                    'unit': '°',
                },
                {
                    'name': 'Maximum wind speed',
                    'convert': lambda x: (x[6] - 32768) / 100,
                    'unit': 'm⋅s⁻¹',
                },
                {
                    'name': 'Air temperature',
                    'convert': lambda x: (x[7] - 32768) / 10,
                    'unit': '°C',
                },
                {
                    'name': 'Vapor pressure',
                    'convert': lambda x: (x[8] - 32768) / 100,
                    'unit': 'kPa',
                },
                {
                    'name': 'Atmospheric pressure',
                    'convert': lambda x: (x[9] - 32768) / 100,
                    'unit': 'kPa',
                },
                {
                    'name': 'Relative humidity',
                    'convert': lambda x: (x[10] - 32768) / 10,
                    'unit': '%',
                },
                {
                    'name': 'Sensor temperature (internal)',
                    'convert': lambda x: (x[11] - 32768) / 10,
                    'unit': '°C',
                },
                {
                    'name': 'X orientation angle',
                    'convert': lambda x: (x[12] - 32768) / 10,
                    'unit': '°',
                },
                {
                    'name': 'Y orientation angle',
                    'convert': lambda x: (x[13] - 32768) / 10,
                    'unit': '°',
                },
                {
                    'name': 'Compass heading',
                    'convert': lambda x: x[14] - 32768,
                    'unit': '°',
                },
                {
                    'name': 'North wind speed',
                    'convert': lambda x: (x[15] - 32768) / 100,
                    'unit': 'm⋅s⁻¹',
                },
                {
                    'name': 'East wind speed',
                    'convert': lambda x: (x[16] - 32768) / 100,
                    'unit': 'm⋅s⁻¹',
                },
            ],
        },
        {
            'length': 1,
            'values': [{
                'name': 'Battery voltage',
                'convert': lambda x: x[0] / 1000,
                'unit': 'V',
            }],
        },
    ],
}


def _decode(
        msg: bytes,
        protocol_version: int,
        # This has to be SensorContainer
        sensors: list[SensorContainer],
        hex: bool = False,
) -> dict[str, BLGMeasurement | ATM41Measurement | ATM41Measurement]:
    """msg: payload as one of hex string, list, or bytearray"""
    #  TODO: There is 100% as way to solve this with Generics, since the return
    # type is determined by what sensors is passed
    bytes_ = bytearray(binascii.a2b_hex(msg) if hex else msg)
    version = bytes_[0]
    if version != protocol_version:
        raise ValueError(
            f"protocol version {version} doesn't match v2",
        )

    device_id = struct.unpack('>H', bytes_[1:3])[0]
    bin_flags = bin(struct.unpack('>H', bytes_[3:5])[0])
    flags = bin_flags[2:].zfill(struct.calcsize('>H') * 8)[::-1]

    words = [
        struct.unpack('>H', bytes_[i:i + 2])[0]
        for i in range(5, len(bytes_), 2)
    ]

    cur = 0
    result = {'Device ID': device_id, 'Protocol version': version}
    for flag, sensor in zip(flags, sensors):
        if flag != '1':
            continue

        x = words[cur:cur + sensor['length']]
        cur += sensor['length']
        for value in sensor['values']:
            if 'convert' not in value:
                continue

            result[value['name']] = {
                'value': value['convert'](x),
                'unit': value.get('unit', None),
            }

    return result


def decode_BLG(
        msg: bytes,
        hex: bool = False,
        protocol_version: int = 2,
) -> BLGMeasurement:
    """Decode the message returned from the blackglobe sensor (BLG).

    :param msg: byte-string returned e.g. ``b'0254970003498800830BF7'``
    :param hex: whether or not the provided message is in hexadecimals
    :param protocol_version: The expected version of the protocol. If the
        ``protocol_version`` in the ``msg`` does not match this version, an
        exception will be raised
    """
    sensors = DECODER_SCHEMAS['BLG']
    ret = _decode(
        msg=msg,
        protocol_version=protocol_version,
        sensors=sensors,
        hex=hex,
    )
    return cast(BLGMeasurement, ret)


def decode_STH35(
        msg: bytes,
        hex: bool = False,
        protocol_version: int = 2,
) -> SHT35Measurement:
    """Decode the message returned from the temperature and relative humidity
    sensor (SHT35).

    :param msg: byte-string returned e.g. ``b'0254A60003783F596E0C17'``
    :param hex: whether or not the provided message is in hexadecimals
    :param protocol_version: The expected version of the protocol. If the
        ``protocol_version`` in the ``msg`` does not match this version, an
        exception will be raised
    """
    sensors = DECODER_SCHEMAS['SHT35']
    ret = _decode(
        msg=msg,
        protocol_version=protocol_version,
        sensors=sensors,
        hex=hex,
    )
    return cast(SHT35Measurement, ret)


def decode_ATM41(
        msg: bytes,
        hex: bool = False,
        protocol_version: int = 2,
) -> ATM41Measurement:
    """Decode the message returned from the Meter ATM41 weather station (ATM41)
    sensor.

    :param msg: byte-string returned e.g.
        ``b'02530400038283800080008000803488CD8076815C80CBA708816D817D80197FF680007FDB7FDB0AAE'``
    :param hex: whether or not the provided message is in hexadecimals
    :param protocol_version: The expected version of the protocol. If the
        ``protocol_version`` in the ``msg`` does not match this version, an
        exception will be raised
    """
    sensors = DECODER_SCHEMAS['ATM41']
    ret = _decode(
        msg=msg,
        protocol_version=protocol_version,
        sensors=sensors,
        hex=hex,
    )
    return cast(ATM41Measurement, ret)
