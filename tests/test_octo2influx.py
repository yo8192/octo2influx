import sys
import os

sys.path.append(os.path.abspath('./src'))  # to run from top dir
sys.path.append(os.path.abspath('../src'))  # to run from tests/ dir

import pytest

from datetime import datetime, timedelta, timezone
from freezegun import freeze_time
import pytz
import octo2influx
from octo2influx import cfg

@pytest.fixture
def load_example_config():
    cfg.clear()
    for directory in '../src', 'src':
        path = os.path.join(directory, 'config.example.yaml')
        if os.path.isfile(path):
            cfg.set_file(path)
            return

    raise FileNotFoundError("Could not find 'config.example.yaml'")

def test_load_config(load_example_config):
    # This also tests the validation of each of these valid config items:
    assert cfg['timezone'] == 'Europe/London'
    assert cfg['from_max_days_ago'] == 60
    assert cfg['loglevel'] == 'INFO'

def test_datetime_days_ago(load_example_config):
    cfg_tz = pytz.timezone(cfg['timezone'])
    for d in ['2024-01-10 12:34:56', '2024-01-10 00:00:00', '2024-01-10 23:59:59']:
        with freeze_time(d):
            assert octo2influx.datetime_days_ago(0, datetime.min.time()) == cfg_tz.localize(datetime(2024, 1, 10, 0, 0, 0))
            assert octo2influx.datetime_days_ago(0, datetime.max.time()) == cfg_tz.localize(datetime(2024, 1, 10, 23, 59, 59, 999999))

            assert octo2influx.datetime_days_ago(1, datetime.min.time()) == cfg_tz.localize(datetime(2024, 1, 9, 0, 0, 0))
            assert octo2influx.datetime_days_ago(5, datetime.min.time()) == cfg_tz.localize(datetime(2024, 1, 5, 0, 0, 0))

def test_datetime_from_days_ago(load_example_config):
    cfg_tz = pytz.timezone(cfg['timezone'])
    for d in ['2024-01-10 12:34:56', '2024-01-10 00:00:00', '2024-01-10 23:59:59']:
        with freeze_time(d):
            assert octo2influx.datetime_from_days_ago(0) == cfg_tz.localize(datetime(2024, 1, 10, 0, 0, 0))
            assert octo2influx.datetime_from_days_ago(1) == cfg_tz.localize(datetime(2024, 1, 9, 0, 0, 0))
            assert octo2influx.datetime_from_days_ago(5) == cfg_tz.localize(datetime(2024, 1, 5, 0, 0, 0))

def test_datetime_to_days_ago(load_example_config):
    cfg_tz = pytz.timezone(cfg['timezone'])
    for d in ['2024-01-10 12:34:56', '2024-01-10 00:00:00', '2024-01-10 23:59:59']:
        with freeze_time(d):
            assert octo2influx.datetime_to_days_ago(0) == cfg_tz.localize(datetime(2024, 1, 10, 23, 59, 59, 999999))
            assert octo2influx.datetime_to_days_ago(1) == cfg_tz.localize(datetime(2024, 1, 9, 23, 59, 59, 999999))
            assert octo2influx.datetime_to_days_ago(5) == cfg_tz.localize(datetime(2024, 1, 5, 23, 59, 59, 999999))


def test_iso8601_from_datetime():
    london = pytz.timezone('Europe/London')
    # Summer time (BST):
    assert octo2influx.iso8601_from_datetime(london.localize(datetime(2023, 6, 2, 15, 0, 0))) == '2023-06-02T14:00:00Z'
    assert octo2influx.iso8601_from_datetime(london.localize(datetime(2023, 6, 2, 00, 30, 0))) == '2023-06-01T23:30:00Z'

    # Winter time (GMT):
    assert octo2influx.iso8601_from_datetime(london.localize(datetime(2024, 1, 10, 15, 0, 0))) == '2024-01-10T15:00:00Z'
    assert octo2influx.iso8601_from_datetime(london.localize(datetime(2024, 1, 10, 00, 30, 0))) == '2024-01-10T00:30:00Z'


def     test_std_unit_rate_to_points_long_point_validity(load_example_config):
    row = {'value_exc_vat': 34.7988, 'value_inc_vat': 36.53874, 'valid_from': '2023-03-31T23:00:00Z', 'valid_to': '2024-01-01T00:00:00Z', 'payment_method': None}
    london = pytz.timezone('Europe/London')
    from_dt = london.localize(datetime(2023, 12, 17, 00, 00))
    to_dt = london.localize(datetime(2023, 12, 22, 23, 59, 59))
    points = octo2influx.std_unit_rate_to_points('octopus-tariffs', row, 'standing-charges', 'p/day', cfg['tariffs'][3], from_dt, to_dt)
    expected_str_points = [
        # one point per day from from_dt (2023-12-17) to valid_to (2024-01-01T00:00):
        'octopus-tariffs,direction=import,display_name=Octopus\\ Flux\\ Import,energy_type=electricity,price_type=standing-charges,product_code=FLUX-IMPORT-23-02-14,tariff_code=E-1R-FLUX-IMPORT-23-02-14-C p/day_exc_vat=34.7988,p/day_inc_vat=36.53874 1702771200000000000',
        'octopus-tariffs,direction=import,display_name=Octopus\\ Flux\\ Import,energy_type=electricity,price_type=standing-charges,product_code=FLUX-IMPORT-23-02-14,tariff_code=E-1R-FLUX-IMPORT-23-02-14-C p/day_exc_vat=34.7988,p/day_inc_vat=36.53874 1702857600000000000',
        'octopus-tariffs,direction=import,display_name=Octopus\\ Flux\\ Import,energy_type=electricity,price_type=standing-charges,product_code=FLUX-IMPORT-23-02-14,tariff_code=E-1R-FLUX-IMPORT-23-02-14-C p/day_exc_vat=34.7988,p/day_inc_vat=36.53874 1702944000000000000',
        'octopus-tariffs,direction=import,display_name=Octopus\\ Flux\\ Import,energy_type=electricity,price_type=standing-charges,product_code=FLUX-IMPORT-23-02-14,tariff_code=E-1R-FLUX-IMPORT-23-02-14-C p/day_exc_vat=34.7988,p/day_inc_vat=36.53874 1703030400000000000',
        'octopus-tariffs,direction=import,display_name=Octopus\\ Flux\\ Import,energy_type=electricity,price_type=standing-charges,product_code=FLUX-IMPORT-23-02-14,tariff_code=E-1R-FLUX-IMPORT-23-02-14-C p/day_exc_vat=34.7988,p/day_inc_vat=36.53874 1703116800000000000',
        'octopus-tariffs,direction=import,display_name=Octopus\\ Flux\\ Import,energy_type=electricity,price_type=standing-charges,product_code=FLUX-IMPORT-23-02-14,tariff_code=E-1R-FLUX-IMPORT-23-02-14-C p/day_exc_vat=34.7988,p/day_inc_vat=36.53874 1703203200000000000',
        'octopus-tariffs,direction=import,display_name=Octopus\\ Flux\\ Import,energy_type=electricity,price_type=standing-charges,product_code=FLUX-IMPORT-23-02-14,tariff_code=E-1R-FLUX-IMPORT-23-02-14-C p/day_exc_vat=34.7988,p/day_inc_vat=36.53874 1703289600000000000',
        'octopus-tariffs,direction=import,display_name=Octopus\\ Flux\\ Import,energy_type=electricity,price_type=standing-charges,product_code=FLUX-IMPORT-23-02-14,tariff_code=E-1R-FLUX-IMPORT-23-02-14-C p/day_exc_vat=34.7988,p/day_inc_vat=36.53874 1703376000000000000',
        'octopus-tariffs,direction=import,display_name=Octopus\\ Flux\\ Import,energy_type=electricity,price_type=standing-charges,product_code=FLUX-IMPORT-23-02-14,tariff_code=E-1R-FLUX-IMPORT-23-02-14-C p/day_exc_vat=34.7988,p/day_inc_vat=36.53874 1703462400000000000',
        'octopus-tariffs,direction=import,display_name=Octopus\\ Flux\\ Import,energy_type=electricity,price_type=standing-charges,product_code=FLUX-IMPORT-23-02-14,tariff_code=E-1R-FLUX-IMPORT-23-02-14-C p/day_exc_vat=34.7988,p/day_inc_vat=36.53874 1703548800000000000',
        'octopus-tariffs,direction=import,display_name=Octopus\\ Flux\\ Import,energy_type=electricity,price_type=standing-charges,product_code=FLUX-IMPORT-23-02-14,tariff_code=E-1R-FLUX-IMPORT-23-02-14-C p/day_exc_vat=34.7988,p/day_inc_vat=36.53874 1703635200000000000',
        'octopus-tariffs,direction=import,display_name=Octopus\\ Flux\\ Import,energy_type=electricity,price_type=standing-charges,product_code=FLUX-IMPORT-23-02-14,tariff_code=E-1R-FLUX-IMPORT-23-02-14-C p/day_exc_vat=34.7988,p/day_inc_vat=36.53874 1703721600000000000',
        'octopus-tariffs,direction=import,display_name=Octopus\\ Flux\\ Import,energy_type=electricity,price_type=standing-charges,product_code=FLUX-IMPORT-23-02-14,tariff_code=E-1R-FLUX-IMPORT-23-02-14-C p/day_exc_vat=34.7988,p/day_inc_vat=36.53874 1703808000000000000',
        'octopus-tariffs,direction=import,display_name=Octopus\\ Flux\\ Import,energy_type=electricity,price_type=standing-charges,product_code=FLUX-IMPORT-23-02-14,tariff_code=E-1R-FLUX-IMPORT-23-02-14-C p/day_exc_vat=34.7988,p/day_inc_vat=36.53874 1703894400000000000',
        'octopus-tariffs,direction=import,display_name=Octopus\\ Flux\\ Import,energy_type=electricity,price_type=standing-charges,product_code=FLUX-IMPORT-23-02-14,tariff_code=E-1R-FLUX-IMPORT-23-02-14-C p/day_exc_vat=34.7988,p/day_inc_vat=36.53874 1703980800000000000',
        'octopus-tariffs,direction=import,display_name=Octopus\\ Flux\\ Import,energy_type=electricity,price_type=standing-charges,product_code=FLUX-IMPORT-23-02-14,tariff_code=E-1R-FLUX-IMPORT-23-02-14-C p/day_exc_vat=34.7988,p/day_inc_vat=36.53874 1704067199000000000'
    ]
    assert [p.to_line_protocol() for p in points] == expected_str_points


def test_std_unit_rate_to_points_short_point_validity(load_example_config):
    row = {'value_exc_vat': 37.9043, 'value_inc_vat': 39.799515, 'valid_from': '2023-12-19T16:00:00Z', 'valid_to': '2023-12-19T19:00:00Z', 'payment_method': None}
    london = pytz.timezone('Europe/London')
    from_dt = london.localize(datetime(2023, 12, 17, 00, 00))
    to_dt = london.localize(datetime(2023, 12, 22, 23, 59, 59))
    points = octo2influx.std_unit_rate_to_points('octopus-tariffs', row, "standard-unit-rates", "p/kWh", cfg['tariffs'][3], from_dt, to_dt)
    expected_str_points = [
        'octopus-tariffs,direction=import,display_name=Octopus\\ Flux\\ Import,energy_type=electricity,price_type=standard-unit-rates,product_code=FLUX-IMPORT-23-02-14,tariff_code=E-1R-FLUX-IMPORT-23-02-14-C p/kWh_exc_vat=37.9043,p/kWh_inc_vat=39.799515 1703001600000000000',
        'octopus-tariffs,direction=import,display_name=Octopus\\ Flux\\ Import,energy_type=electricity,price_type=standard-unit-rates,product_code=FLUX-IMPORT-23-02-14,tariff_code=E-1R-FLUX-IMPORT-23-02-14-C p/kWh_exc_vat=37.9043,p/kWh_inc_vat=39.799515 1703012399000000000'
    ]
    assert [p.to_line_protocol() for p in points] == expected_str_points


def test_consumption_to_point(load_example_config):
    row = {'consumption': 1.214, 'interval_start': '2023-12-19T04:30:00Z', 'interval_end': '2023-12-19T05:00:00Z'}
    point = octo2influx.consumption_to_point('octopus-usage', row, cfg['usage'][0])
    expected_point_str = 'octopus-usage,direction=import,energy_type=electricity,meter_point=mpan,meter_serial=serial_number interval_end=1702962000,interval_start=1702960200,kWh=1.214 1702961100000000000'
    assert point.to_line_protocol() == expected_point_str

def test_consumption_to_point_electricty_summer(load_example_config):
    row = {'consumption': 0.001, 'interval_start': '2023-08-30T00:00:00+01:00', 'interval_end': '2023-08-30T00:30:00+01:00'}
    point = octo2influx.consumption_to_point('octopus-usage', row, cfg['usage'][0])
    expected_point_str = 'octopus-usage,direction=import,energy_type=electricity,meter_point=mpan,meter_serial=serial_number interval_end=1693351800,interval_start=1693350000,kWh=0.001 1693350900000000000'
    assert point.to_line_protocol() == expected_point_str

def test_consumption_to_point_gas(load_example_config):
    row = {'consumption': 0.0, 'interval_start': '2023-12-14T23:30:00Z', 'interval_end': '2023-12-15T00:00:00Z'}
    point = octo2influx.consumption_to_point('octopus-usage', row, cfg['usage'][2])
    expected_point_str = 'octopus-usage,direction=import,energy_type=gas,meter_point=mprn,meter_serial=serial_number interval_end=1702598400,interval_start=1702596600,m3=0 1702597500000000000'
    assert point.to_line_protocol() == expected_point_str
