import pytest

from element.parsers import decode_ATM41
from element.parsers import decode_BLG
from element.parsers import decode_STH35


def test_decode_STH35() -> None:
    data = decode_STH35(msg=b'0254A60003783F596E0C17', hex=True)
    assert data == {
        'Air humidity': {'unit': '%', 'value': 34.934004730296785},
        'Air temperature': {'unit': '°C', 'value': 37.200732433051044},
        'Battery voltage': {'unit': 'V', 'value': 3.095},
        'Device ID': 21670,
        'Protocol version': 2,
    }


def test_decode_STH35_incorrect_protocol_version() -> None:
    with pytest.raises(ValueError) as exc_info:
        decode_STH35(
            msg=b'0254A60003783F596E0C17',
            hex=True,
            protocol_version=1,
        )
    msg, = exc_info.value.args
    assert msg == "protocol version 2 doesn't match v2"


def test_decode_BLG() -> None:
    data = decode_BLG(msg=b'0254970003498800830BF7', hex=True)
    assert data == {
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


def test_decode_ATM431() -> None:
    data = decode_ATM41(
        msg=b'02530400038283800080008000803488CD8076815C80CBA708816D817D80197FF680007FDB7FDB0AAE',  # noqa: E501
        hex=True,
    )
    assert data == {
        'Air temperature': {
            'unit': '°C',
            'value': 34.8,
        },
        'Atmospheric pressure': {
            'unit': 'kPa',
            'value': 99.92,
        },
        'Battery voltage': {
            'unit': 'V',
            'value': 2.734,
        },
        'Compass heading': {
            'unit': '°',
            'value': 0,
        },
        'Device ID': 21252,
        'East wind speed': {
            'unit': 'm⋅s⁻¹',
            'value': -0.37,
        },
        'Lightning average distance': {
            'unit': 'km',
            'value': 0,
        },
        'Lightning strike count': {
            'unit': None,
            'value': 0,
        },
        'Maximum wind speed': {
            'unit': 'm⋅s⁻¹',
            'value': 1.18,
        },
        'North wind speed': {
            'unit': 'm⋅s⁻¹',
            'value': -0.37,
        },
        'Precipitation': {
            'unit': 'mm',
            'value': 0.0,
        },
        'Protocol version': 2,
        'Relative humidity': {
            'unit': '%',
            'value': 36.5,
        },
        'Sensor temperature (internal)': {
            'unit': '°C',
            'value': 38.1,
        },
        'Solar radiation': {
            'unit': 'W⋅m⁻²',
            'value': 643,
        },
        'Vapor pressure': {
            'unit': 'kPa',
            'value': 2.03,
        },
        'Wind direction': {
            'unit': '°',
            'value': 225.3,
        },
        'Wind speed': {
            'unit': 'm⋅s⁻¹',
            'value': 0.52,
        },
        'X orientation angle': {
            'unit': '°',
            'value': 2.5,
        },
        'Y orientation angle': {
            'unit': '°',
            'value': -1.0,
        },
    }
