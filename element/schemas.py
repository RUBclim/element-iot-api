from typing import Any
from typing import Generic
from typing import Literal
from typing import NotRequired
from typing import TypedDict
from typing import TypeVar

# define the json schemas as TypedDicts
# to have nice type checking and autocomplete


class SHT35Data(TypedDict):
    air_humidity: float
    air_temperature: float
    battery_voltage: float
    device_id: int
    protocol_version: int


class BLGData(TypedDict):
    temperature: float
    thermistor_resistance: float
    voltage_ratio: float
    battery_voltage: float
    device_id: int
    protocol_version: int


class ATM41Data(TypedDict):
    air_temperature: float
    atmospheric_pressure: float
    battery_voltage: float
    compass_heading: int
    device_id: int
    east_wind_speed: float
    lightning_average_distance: float
    lightning_strike_count: int
    maximum_wind_speed: float
    north_wind_speed: float
    precipitation: float
    protocol_version: int
    relative_humidity: float
    sensor_temperature_internal: float
    solar_radiation: float
    vapor_pressure: float
    wind_direction: float
    wind_speed: float
    x_orientation_angle: float
    y_orientation_angle: float


class Reading(TypedDict):
    parser_id: str
    device_id: str
    packet_id: str
    location: str | None
    inserted_at: str
    measured_at: str
    data: SHT35Data | BLGData | ATM41Data
    id: str


class Folder(TypedDict):
    stats: str | None
    updated_at: str
    inserted_at: str
    group_interface_id: str | None
    default_devices_view_id: str | None
    default_packets_view_id: str | None
    default_readings_view_id: str | None
    default_graph_preset_id: str | None
    default_layers_id: str | None
    parent_id: str | None
    mandate_id: str | None
    path: list[str]
    slug: str
    color_hue: int
    description: str | None
    name: str
    id: str


class SessionContext(TypedDict):
    a_fcnt_down: int
    app_s_key: str
    dev_addr: str
    f_nwk_s_int_key: str
    fcnt_down: int | None
    fcnt_up: int
    n_fcnt_down: int
    nwk_s_enc_key: str
    nwk_s_key: str | None
    s_nwk_s_int_key: str


class Opts(TypedDict):
    app_session_key: str
    check_fcnt: bool
    check_join_eui: bool
    class_c: bool
    device_address: str
    device_eui: str
    device_key: str
    device_type: str
    gw_whitelist: str | None
    join_eui: str
    lns_session_context: SessionContext
    max_adr_steps_per_change: int
    net_id: str | None
    network_session_key: str
    region: str
    rx2_dr: int
    rx_delay: int


class Interface(TypedDict):
    id: str
    opts: Opts
    meta: str | None
    deleted_at: str | None
    driver_instance_id: str
    device_id: str
    enabled: bool


class PacketInterval(TypedDict):
    days: int
    months: int
    secs: int
    microsecs: int


class Stats(TypedDict):
    expires_at: str | None
    dirty: str | None
    mandate_id: str
    last_probe_ping: str | None
    last_packet_forwarder_ping: str | None
    missed_up_frames: str
    nominally_sending: bool
    avg_gw_count: str
    packet_interval: PacketInterval
    transceived_at: str
    avg_sf: float
    avg_rssi: float
    avg_snr: float
    id: str


class GatewayStats(TypedDict):
    router_id: int
    router_id_hex: str
    rssi: int
    snr: float
    tmst: int


class _DeviceInfo(TypedDict):
    bemerkung: NotRequired[str]
    geratetyp: str
    hersteller: str
    installiert_von: NotRequired[str]
    ort: NotRequired[str]
    plz: NotRequired[int]
    seriennummer: str  # this is stupid! it's an integer as a string...
    strasse: NotRequired[str]
    hausnummer: NotRequired[str]


class _Fields(TypedDict):
    gerateinformation: _DeviceInfo


class Location(TypedDict):
    coordinates: list[float]
    type: Literal['Point']


class Device(TypedDict):
    name: str
    slug: str
    location: Location | None
    static_location: bool
    icon: str
    inserted_at: str
    updated_at: str
    interfaces: list[Interface]
    tags: list[Folder]
    stats: Stats
    last_readings: list[Any]  # TODO:
    deleted_at: str | None
    default_packets_view_id: str | None
    default_readings_view_id:  str | None
    template_id:  str | None
    default_graph_preset_id:  str | None
    default_layers_id:  str | None
    mandate_id:  str
    parser_id: str
    meta:  str | None
    id: str
    fields: NotRequired[_Fields]
    profile_data: NotRequired[Any]


class RegionMeta(TypedDict):
    bandwidth: int
    bitrate: int
    code: str
    datarate: int
    name: str
    spreadingfactor: int


class Meta(TypedDict):
    ack: bool
    adr: bool
    adr_ack_req: bool
    codr: str
    confirm: bool
    data_rate: int
    datr: str
    dev_nonce: int | None
    frame_count_up: int
    frame_port: int
    frequency: float
    gateway_stats: list[GatewayStats]
    lns_packet_uuid: str
    lorawan_toa_ms: float
    mac_commands: list[Any]
    modu: str
    region: str
    region_meta: RegionMeta
    size: int
    stat: int


class Packet(TypedDict):
    id: str
    payload: bytes | None
    payload_encoding: Literal['json', 'binary', 'utf8'] | None
    packet_type: Literal['up', 'down']
    meta: Meta
    transceived_at: str
    inserted_at: str
    is_meta: NotRequired[bool]
    skip_parsers: bool
    skip_rules: bool
    driver: str | None
    interface_id: str
    device_id: str


T = TypeVar('T')


class ApiReturn(TypedDict, Generic[T]):
    """The generic structure of a return value from the API."""
    body: T
    ok: bool
    retrieve_after_id: NotRequired[str]
    status: int
