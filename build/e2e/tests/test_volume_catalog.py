"""Managed Volume acceptance must check catalog state and preserve cleanup."""
import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]


class VolumeCatalogTests(unittest.TestCase):
    def fixture(self):
        environment = Mock()
        volume = types.SimpleNamespace(uid='uid')

        def ensure(definition, **_options):
            volume.spec = definition
            return volume

        environment.ensure_volume.side_effect = ensure
        environment.get_volume.return_value = volume
        sdk = types.SimpleNamespace(
            Environment=environment,
            EnvironmentNotFound=RuntimeError,
            VolumeSpec=types.SimpleNamespace,
            DirectoryVolumeSource=types.SimpleNamespace,
        )
        module_spec = importlib.util.spec_from_file_location('volume_catalog_case', ROOT / 'functional_lifecycle.py')
        scenario = importlib.util.module_from_spec(module_spec)
        with patch.dict(sys.modules, {'adx_environment': sdk}):
            module_spec.loader.exec_module(scenario)
        return scenario, environment, volume

    def test_paginated_catalog_loses_the_deleted_uid(self):
        scenario, environment, volume = self.fixture()
        environment.list_volumes.side_effect = [([], 'next'), ([volume], ''), ([], '')]
        self.assertEqual(scenario.volume_catalog(object()), 'uid')
        self.assertEqual(environment.ensure_volume.call_count, 2)
        environment.get_volume.assert_called_once()
        environment.delete_volume.assert_called_once()
        self.assertEqual(environment.list_volumes.call_args_list[1].kwargs['page_token'], 'next')

    def test_missing_created_volume_fails_and_cleans_up(self):
        scenario, environment, _volume = self.fixture()
        environment.list_volumes.return_value = ([], '')
        with self.assertRaisesRegex(AssertionError, 'new volume missing'):
            scenario.volume_catalog(object())
        environment.delete_volume.assert_called_once()

    def test_delete_must_remove_the_catalog_entry(self):
        scenario, environment, volume = self.fixture()
        environment.list_volumes.return_value = ([volume], '')
        with self.assertRaisesRegex(AssertionError, 'deleted volume remains'):
            scenario.volume_catalog(object())
        environment.delete_volume.assert_called_once()


if __name__ == '__main__':
    unittest.main()
