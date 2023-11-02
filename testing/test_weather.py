import unittest
from extract_weather_data import test


class TestMethods(unittest.TestCase):
    def test_main(self):
        test()
        self.assertTrue(True)


if __name__ == "__main__":
    unittest.main()
