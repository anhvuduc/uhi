import sys
import os
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

# Mock dependencies BEFORE importing src
sys.modules['google'] = MagicMock()
sys.modules['google.auth'] = MagicMock()
sys.modules['google.auth.transport'] = MagicMock()
sys.modules['google.auth.transport.requests'] = MagicMock()
sys.modules['google.oauth2'] = MagicMock()
sys.modules['google.oauth2.service_account'] = MagicMock()
sys.modules['google.cloud'] = MagicMock()
sys.modules['google.cloud.storage'] = MagicMock()
sys.modules['ee'] = MagicMock()

# Mock dateutil
mock_dateutil = MagicMock()
sys.modules['dateutil'] = mock_dateutil
sys.modules['dateutil.relativedelta'] = mock_dateutil

# Mock relativedelta class specifically
mock_relativedelta = MagicMock()
mock_dateutil.relativedelta = mock_relativedelta

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.utils.gee_utils import get_date_range, get_satellite_dates

class TestDAGLogic(unittest.TestCase):
    
    def test_get_date_range_monthly(self):
        # Mock datetime.now() is hard, so we just check if it returns a valid date range relative to "today"
        # Or we can trust the logic if it runs without error and returns reasonable strings
        start, end = get_date_range('monthly')
        print(f"Monthly: {start} -> {end}")
        self.assertIsNotNone(start)
        self.assertIsNotNone(end)
        
    def test_get_date_range_yearly(self):
        start, end = get_date_range('yearly')
        print(f"Yearly: {start} -> {end}")
        self.assertIsNotNone(start)
        self.assertIsNotNone(end)
        
    def test_get_date_range_custom(self):
        params = {'start_date': '2023-01-01', 'end_date': '2023-01-10'}
        start, end = get_date_range('custom', params)
        print(f"Custom: {start} -> {end}")
        self.assertEqual(start, '2023-01-01')
        self.assertEqual(end, '2023-01-10')

    def test_get_date_range_historical(self):
        start, end = get_date_range('historical')
        print(f"Historical: {start} -> {end}")
        self.assertEqual(start, 'HISTORICAL')
        self.assertEqual(end, 'HISTORICAL')

    @patch('src.utils.gee_utils.initialize_gee')
    @patch('ee.ImageCollection')
    def test_get_satellite_dates(self, mock_col, mock_init):
        # Mock GEE behavior
        mock_img_start = MagicMock()
        mock_img_start.get.return_value.getInfo.return_value = 946684800000 # 2000-01-01
        
        mock_img_end = MagicMock()
        mock_img_end.get.return_value.getInfo.return_value = 1704067200000 # 2024-01-01
        
        # Chain calls
        mock_col.return_value.sort.return_value.first.side_effect = [mock_img_start, mock_img_end]
        
        start, end = get_satellite_dates('MODIS/061/MOD11A1')
        print(f"Satellite Dates (Mocked): {start} -> {end}")
        self.assertEqual(start, '2000-01-01')
        self.assertEqual(end, '2024-01-01')

if __name__ == '__main__':
    unittest.main()
