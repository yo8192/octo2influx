#!/usr/bin/python3

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client import query_api
from influxdb_client.client.write_api import SYNCHRONOUS
import dateutil.parser
from datetime import date, datetime, timedelta, timezone
import pytz
import requests
from urllib import parse
from urllib3 import Retry
import argparse
import confuse
from dataclasses import dataclass
from os import path
import logging

PROGNAME = 'octo2influx'

logging.basicConfig(level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S', style='{',
                    format='{asctime} {levelname:>7} {filename}:{lineno:3}: {message}')


@dataclass
class Parameter:
    arg_type: type
    cfg_type: confuse.Template
    help: str
    default: any = None
    validator: callable = None


confuse_usage_template = {
    'energy_type': confuse.Choice(["electricity", "gas"]),
    'direction': confuse.Choice(["import", "export"]),
    'meter_point': str,  # MPAN for electricity, MPRN for gas
    'meter_serial': str,
    'unit': confuse.Choice(["kWh", "m3"]),
}


confuse_tariff_template = {
    'energy_type': confuse.Choice(["electricity", "gas"]),
    'direction': confuse.Choice(["import", "export"]),
    'product_code': str,
    'tariff_code': str,
    'full_name': str,
    'display_name': str,
    'description': str,
}


def _secret_unsafe_on_cmdline(val: str):
    raise argparse.ArgumentTypeError(
        'Do not set secrets on the command line as it is not safe: they may be recorded in your shell history, system audit, etc. Use a access-restricted configuration file, or environment variables (e.g. when using Docker Compose).')


def _config_only(val: str):
    raise argparse.ArgumentTypeError(
        'this config key is only supported in a configuration file.')


params = {
    # Runtime parameters:
    'from_max_days_ago': Parameter(int, int, 'Get Octopus data from the last retrieved timestamp, but no more than this many days ago.', default=600, validator=lambda x: x >= 0),
    'from_days_ago': Parameter(int, int, 'Get Octopus data from that many days ago (0 means today). If set, this overrides from_max_days_ago.', validator=lambda x: x >= 0),
    'to_days_ago': Parameter(int, int, 'Get Octopus data until that many days ago (0 means today).', default=0, validator=lambda x: x >= 0),
    'loglevel': Parameter(str, confuse.Choice(['INFO', 'DEBUG', 'WARNING', 'ERROR']), 'Level of logs (INFO, DEBUG, WARNING, ERROR).', default='INFO'),

    # Octopus settings:
    'timezone': Parameter(str, str, 'Timezone of the Octopus account (e.g. where you live). Most likely always "Europe/London".', default="Europe/London"),
    'base_url': Parameter(str, str, 'Base URL of the Octopus API (e.g. "https://api.octopus.energy/v1").'),
    'octopus_api_key': Parameter(_secret_unsafe_on_cmdline, str, '(**Config file or environment only**) The API Token to connect to the Octopus API. Can be generated on https://octopus.energy/dashboard/developer/.'),
    'price_types': Parameter(_config_only, confuse.MappingValues(str), '(**Config only**) List of price types to retrieve using the Octopus API, and their units.'),
    'usage': Parameter(_config_only, confuse.Sequence(confuse_usage_template), '(**Config only**) List of Octopus usage (electricity/gas import consumption, or export) to retrieve using the Octopus API.'),
    'tariffs': Parameter(_config_only, confuse.Sequence(confuse_tariff_template), '(**Config only**) List of Octopus tariffs to retrieve using the Octopus API.'),

    # Influx settings:
    'influx_org': Parameter(str, str, 'InfluxDB 2.X organization name to store the data into.'),
    'influx_bucket': Parameter(str, str, 'InfluxDB 2.X bucket name to store the data into (e.g. "mybucket/autogen").'),
    'influx_tariff_measurement': Parameter(str, str, 'InfluxDB 2.X measurement name to store tariff data into.'),
    'influx_usage_measurement': Parameter(str, str, 'InfluxDB 2.X measurement name to store consumption data into.'),
    'influx_url': Parameter(str, str, 'URL of the InfluxDB 2.X instance to store the data into (e.g. "http://localhost:8086")'),
    'influx_api_token': Parameter(_secret_unsafe_on_cmdline, str, '(**Config file or environment only**) The API Token to connect to the InfluxDB 2.x instance.'),
}

argparse_description = '''
Download usage and pricing data from the Octopus API
(https://developer.octopus.energy/docs/api/) and store into Influxdb.
'''

argparse_epilog = f'''
IMPORTANT NOTE: you should *not* define secrets and API tokens on the command
line, as it is unsecure (e.g. it may stay in your shell history, appear in
system audit logs, etc): you can define in an access-restricted configuration
file instead.

The settings can also be set in a config file (./{confuse.CONFIG_FILENAME},
/etc/{PROGNAME}/{confuse.CONFIG_FILENAME}, ~/.config/{PROGNAME}/{confuse.CONFIG_FILENAME},
or ${PROGNAME.upper()}DIR/{confuse.CONFIG_FILENAME} in a directory of your choice by defining
the env var {PROGNAME.upper()}DIR).
Or via environment variable of the form {PROGNAME.upper()}_COMMAND_LINE_ARG.
The priority from highest to lowest is: environment, command line, config file.
'''


class ValidatedConfiguration(confuse.Configuration):
    """A confuse.Configuration which transparently validates all items.

    Each item with a validator will be transparently validated when
    accessed, with a TypeError exception raised if invalid.
    """

    def __init__(self, params, *args, **kwargs):
        self.params = params
        super().__init__(*args, **kwargs)

    def get_validated(self, key: str):
        assert key in self.params, f"configuration '{key}' not found in params."
        value = super().__getitem__(key).get(self.params[key].cfg_type)
        if self.params[key].validator:
            try:
                self.params[key].validator(value)
            except Exception as e:
                raise TypeError(
                    f"Configuration key '{key}' has an invalid value: {value}") from e

        return value

    def __getitem__(self, key: str):
        return self.get_validated(key)


def get_url_of_tariff(base_url: str, tariff: confuse.templates.AttrDict, price_type: str) -> str:
    return f"{base_url}/products/{tariff.product_code}/{tariff.energy_type}-tariffs/{tariff.tariff_code}/{price_type}/"


def get_url_of_consumption(base_url: str, usage: confuse.templates.AttrDict) -> str:
    """Get the URL to retrieve the consumption.

    Args:
      energy: electricty | gas
      admin_number: MPAN for electricity, MPRN for gas
    """
    return f"{base_url}/{usage.energy_type}-meter-points/{usage.meter_point}/meters/{usage.meter_serial}/consumption/"


def retrieve_paginated_data(
        api_key, url, from_iso8601, to_iso8601, page=None
):
    args = {
        'period_from': from_iso8601,
        'period_to': to_iso8601,
    }
    if page:
        args['page'] = page
    else:
        if cfg['loglevel'] in ['INFO', 'DEBUG']:
            # logging expects full messages, not dot progress, so we print() instead:
            print('    progress (one dot per page) ', end='', flush=True)
    response = requests.get(url, params=args, auth=(api_key, ''))
    response.raise_for_status()
    data = response.json()
    results = data.get('results', [])
    if cfg['loglevel'] in ['INFO', 'DEBUG']:
        print('.', end='', flush=True)
    if data['next']:
        url_query = parse.urlparse(data['next']).query
        next_page = parse.parse_qs(url_query)['page'][0]
        results += retrieve_paginated_data(
            api_key, url, from_iso8601, to_iso8601, next_page
        )
    if not page and cfg['loglevel'] in ['INFO', 'DEBUG']:
        print('\n', end='', flush=True)
    return results


def std_unit_rate_to_points(measurement: str, row: dict, price_type: str, unit: str, tariff: confuse.templates.AttrDict, from_dt: datetime, to_dt: datetime) -> list[Point]:
    """Convert a single Octopus API rate datapoint into multiple InfluxDB points for easier querying and charting.

    Given an Octopus datapoint:
    - if the price has an expiry date: add two influxdb points at times _valid_from and _valid_to-1s.
    - otherwise add one influxdb point per day
    """

    # Example data from the Octopus API:
    # [
    #     {
    #       "value_exc_vat": 23.6849,
    #       "value_inc_vat": 23.6849,
    #       "valid_from": "2023-06-02T18:00:00Z",
    #       "valid_to": "2023-06-03T01:00:00Z",
    #       "payment_method": null
    #     },
    #     {
    #       "value_exc_vat": 37.5588,
    #       "value_inc_vat": 37.5588,
    #       "valid_from": "2023-06-02T15:00:00Z",
    #       "valid_to": "2023-06-02T18:00:00Z",
    #       "payment_method": null
    #     }
    # ]

    def rate2point(tstamp: datetime) -> Point:
        return Point(measurement)\
            .tag("energy_type", tariff.energy_type)\
            .tag("direction", tariff.direction)\
            .tag("tariff_code", tariff.tariff_code)\
            .tag("price_type", price_type)\
            .tag("product_code", tariff.product_code)\
            .tag("display_name", tariff.display_name)\
            .field(f"{unit}_inc_vat", row["value_inc_vat"])\
            .field(f"{unit}_exc_vat", row["value_exc_vat"])\
            .time(tstamp)

    valid_from = from_dt
    if "valid_from" in row and row["valid_from"]:
        point_valid_from = dateutil.parser.isoparse(row["valid_from"])
        # Don't allow points older than from_dt or it might go beyond the Influxdb retention and error:
        if point_valid_from > from_dt:
            valid_from = point_valid_from

    valid_to = to_dt
    if "valid_to" in row and row["valid_to"]:
        valid_to = dateutil.parser.isoparse(
            row["valid_to"])-timedelta(seconds=1)

    to_nextday_dt = valid_to + timedelta(days=1)
    points = []
    cur_dt = valid_from
    while cur_dt < to_nextday_dt:
        if cur_dt >= from_dt - timedelta(days=1):
            points.append(rate2point(cur_dt))

        cur_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        cur_dt += timedelta(days=1)

        if cur_dt > valid_to:
            points.append(rate2point(valid_to))
            break

    return points


def consumption_to_point(measurement: str, row: dict, usage: confuse.templates.AttrDict) -> Point:
    """Convert a single Octopus API usage datapoint into an InfluxDB point."""
    # Example data from the Octopus API:
    # data=[
    # {'consumption': 0.001, 'interval_start': '2023-07-31T00:30:00+01:00', 'interval_end': '2023-07-31T01:00:00+01:00'},
    # {'consumption': 0.0, 'interval_start': '2023-07-31T00:00:00+01:00', 'interval_end': '2023-07-31T00:30:00+01:00'},
    # {'consumption': 0.0, 'interval_start': '2023-07-30T23:30:00+01:00', 'interval_end': '2023-07-31T00:00:00+01:00'},
    # ...
    # ]
    interval_start = dateutil.parser.isoparse(row["interval_start"])
    interval_end = dateutil.parser.isoparse(row["interval_end"])
    mid_dt = interval_start + (interval_end - interval_start) / 2
    return Point(measurement) \
        .tag("energy_type", usage.energy_type)\
        .tag("direction", usage.direction)\
        .tag("meter_point", usage.meter_point)\
        .tag("meter_serial", usage.meter_serial)\
        .field("interval_start", interval_start.timestamp())\
        .field("interval_end", interval_end.timestamp())\
        .field(usage.unit, row["consumption"])\
        .time(mid_dt)


def iso8601_from_datetime(dt: datetime) -> str:
    """Convert a datetime into its iso8601 string representation."""
    dt_utc = dt.astimezone(pytz.utc)
    # We drop the timezone so there is no time offset +HH:MM suffix:
    return f"{dt_utc.replace(tzinfo=None).isoformat(timespec='seconds')}Z"


def datetime_days_ago(days_ago: int, time_of_day: datetime.time) -> datetime:
    """Return the timestamp of days_ago days ago from today at time_of_day."""
    d = datetime.now().date() - timedelta(days=days_ago)
    return pytz.timezone(cfg['timezone']).localize(datetime.combine(d, time_of_day))


def datetime_from_days_ago(days_ago: int) -> datetime:
    """Return the timestamp at 00:00 days_ago days ago."""
    return datetime_days_ago(days_ago, datetime.min.time())


def datetime_to_days_ago(days_ago: int) -> datetime:
    """Return the timestamp at 23:59 days_ago days ago."""
    return datetime_days_ago(days_ago, datetime.max.time())


def query_last_datetime(query_api: query_api,
                        base_query: str, from_max_days_ago: int) -> datetime:
    """Return the timestamp of the most recent point from InfluxDB.

    The function will look for data at most from_max_days_ago old. If none is found,
    it will return the timestamp from from_max_days_ago.
    """
    query = base_query + '''
        |> keep(columns: ["_time"])
        |> sort(columns: ["_time"], desc: false)
        |> last(column: "_time")
        |> yield(name: "last_tstamp")
    '''
    tables = query_api.query(query)
    results = tables.to_values(columns=['_time'])
    if results:
        return results[-1][0]
    else:
        return datetime_from_days_ago(from_max_days_ago)


def tariff_last_datetime(query_api: query_api,
                         influx_bucket: str, from_max_days_ago: int,
                         influx_measurement: str, energy_type: str,
                         price_type: str,
                         tariff_code: str) -> datetime:
    """Return the timestamp of the most recent point from InfluxDB.

    The function will look for data at most from_max_days_ago old. If none is found,
    it will return the timestamp from from_max_days_ago.
    """
    base_query = f'''
        from(bucket: "{influx_bucket}")
        |> range(start: -{from_max_days_ago}d)
        |> filter(fn: (r) => r["_measurement"] == "{influx_measurement}")
        |> filter(fn: (r) => r["energy_type"] == "{energy_type}")
        |> filter(fn: (r) => r["price_type"] == "{price_type}")
        |> filter(fn: (r) => r["tariff_code"] == "{tariff_code}")
    '''
    return query_last_datetime(query_api, base_query, from_max_days_ago)


def consumption_last_iso8601(query_api: query_api,
                             influx_bucket: str, from_max_days_ago: int,
                             influx_measurement: str, energy_type: str,
                             direction: str,
                             meter_point: str, meter_serial: str) -> str:
    """Return the timestamp of the most recent point from InfluxDB, in ISO8601 format.

    The function will look for data at most from_max_days_ago old. If none is found,
    it will return the timestamp from from_max_days_ago.
    """
    base_query = f'''
        from(bucket: "{influx_bucket}")
        |> range(start: -{from_max_days_ago}d)
        |> filter(fn: (r) => r["_measurement"] == "{influx_measurement}")
        |> filter(fn: (r) => r["direction"] == "{direction}")
        |> filter(fn: (r) => r["meter_point"] == "{meter_point}")
        |> filter(fn: (r) => r["meter_serial"] == "{meter_serial}")
    '''
    last_dt = query_last_datetime(query_api, base_query, from_max_days_ago)
    return iso8601_from_datetime(last_dt)


def build_argparser(params: dict[str, Parameter]) -> argparse.ArgumentParser:
    """Build and return a command line argument parser."""
    parser = argparse.ArgumentParser(
        prog=PROGNAME,
        description=argparse_description,
        epilog=argparse_epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    for name, parameter in params.items():
        # Only use the parameter.default if the setting wasn't present
        # in the config file, or the config file setting would be ignored:
        default = parameter.default
        if parameter.default:
            try:
                default = cfg[name]
            except confuse.exceptions.NotFoundError:
                pass
        parser.add_argument(
            f'--{name}', type=parameter.arg_type, help=parameter.help, default=default)

    return parser


cfg = ValidatedConfiguration(params, PROGNAME, __name__)


if __name__ == "__main__":

    # Confuse automatically tries to load config.yaml from a number of
    # locations. Also try to load a config file in the same directory:
    local_config_path = path.join(path.realpath(
        path.dirname(__file__)), confuse.CONFIG_FILENAME)
    read_local_config = False
    try:
        cfg.set_file(local_config_path)
        read_local_config = True
    except confuse.exceptions.ConfigReadError:
        pass

    parser = build_argparser(params)
    args = parser.parse_args()
    cfg.set_args(args)

    cfg.set_env()

    logging.root.setLevel(cfg['loglevel'])

    if read_local_config:
        logging.info(f'Read configuration from {local_config_path}.')
    else:
        # Check confuse did load a config file ok
        try:
            if not cfg['price_types']:
                raise ValueError
        except (confuse.exceptions.NotFoundError, ValueError):
            configfile_paths = [
                local_config_path,
                path.join(f'/etc', PROGNAME, confuse.CONFIG_FILENAME),
                path.join(path.expanduser('~/.config/'),
                            PROGNAME, confuse.CONFIG_FILENAME)
            ]
            raise SystemExit(
                    'Configuration key "price_types" was not found or empty. '
                f'Please check you have a valid configuration file at one of {configfile_paths}.')

    to_dt = datetime_to_days_ago(cfg['to_days_ago'])
    to_iso8601 = iso8601_from_datetime(to_dt)
    try:
        from_days_ago = cfg['from_days_ago']
        from_dt = datetime_from_days_ago(from_days_ago)
        from_iso8601 = iso8601_from_datetime(from_dt)
        logging.info(
            f'`from_days_ago` is defined: retrieving from {from_days_ago} days ago.')
    except confuse.exceptions.NotFoundError:
        from_days_ago = None

    client = InfluxDBClient(url=cfg['influx_url'],
                            token=cfg['influx_api_token'], org=cfg['influx_org'],
                            retries=Retry(connect=5, read=4, backoff_factor=0.7))
    write_api = client.write_api(write_options=SYNCHRONOUS)
    query_api = client.query_api()

    # Get consumption
    logging.info('=== Retrieving consumption...')
    for usage in cfg['usage']:
        consumption_url = get_url_of_consumption(cfg['base_url'], usage)
        logging.debug(f'API URL: {consumption_url}')

        if from_days_ago is None:
            from_iso8601 = consumption_last_iso8601(
                query_api, cfg['influx_bucket'], cfg['from_max_days_ago'],
                cfg['influx_usage_measurement'], usage.energy_type, usage.direction,
                usage.meter_point, usage.meter_serial)

        logging.info(f'====== Retrieving {usage.energy_type} {usage.direction} ({usage.meter_point}) from Octopus...')
        logging.debug(f"from {from_iso8601} to {to_iso8601}")
        data = retrieve_paginated_data(
            cfg['octopus_api_key'], consumption_url, from_iso8601, to_iso8601)

        logging.info(
            f'       ... {len(data)} points retrieved from Octopus.')

        logging.info(f'====== Writing {usage.energy_type} {usage.direction} ({usage.meter_point}) to Influx...')
        points = []
        # we receive the data from Octopus from newest to oldest - we reverse this:
        # (in particular this ensures we won't have a gap if we fail in the middle
        # of writing and then start again from the newest written point)
        for row in reversed(data):
            points.append(consumption_to_point(
                cfg['influx_usage_measurement'], row, usage))

        if cfg['loglevel'] == 'DEBUG':
            logging.debug("\n" + "\n".join([p.to_line_protocol() for p in points]))

        write_api.write(bucket=cfg['influx_bucket'], record=points)
        logging.info(
            f'       ... {len(points)} points written to Influx.')


    logging.info('=== Retrieving tariffs...')
    for tariff in cfg['tariffs']:
        for price_type, unit in cfg['price_types'].items():
            url = get_url_of_tariff(cfg['base_url'], tariff, price_type)

            if from_days_ago is None:
                from_dt = tariff_last_datetime(
                    query_api, cfg['influx_bucket'], cfg['from_max_days_ago'],
                    cfg['influx_tariff_measurement'], tariff.energy_type, price_type, tariff.tariff_code)
                from_iso8601 = iso8601_from_datetime(from_dt)

            logging.info(f'====== Retrieving {tariff.energy_type} {price_type} price of tariff {tariff.full_name} from Octopus...')
            logging.debug(f"from {from_iso8601} to {to_iso8601}")
            data = retrieve_paginated_data(
                cfg['octopus_api_key'], url, from_iso8601, to_iso8601)
            if cfg['loglevel'] == 'DEBUG':
                logging.debug("\n" + "\n".join([str(point) for point in data]))
            logging.info(
                f'       ... {len(data)} points retrieved from Octopus.')

            logging.info(f'====== Writing {tariff.energy_type} {price_type} price of tariff {tariff.full_name} to Influx...')
            logging.debug(f"from {from_dt} to {to_dt}")
            points = []
            # we receive the data from Octopus from newest to oldest - we reverse this:
            # (in particular this ensures we won't have a gap if we fail in the middle
            # of writing and then start again from the newest written point)
            for r in reversed(data):
                points.extend(std_unit_rate_to_points(
                    cfg['influx_tariff_measurement'], r, price_type, unit, tariff, from_dt, to_dt))

            if cfg['loglevel'] == 'DEBUG':
                logging.debug("\n" + "\n".join([p.to_line_protocol() for p in points]))

            write_api.write(bucket=cfg['influx_bucket'], record=points)
            logging.info(
                f'       ... {len(points)} points written to Influx '
                '(including any extra points for easier querying and better charting).')
