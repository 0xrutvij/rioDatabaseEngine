import unittest

class objectless(object):
    def __new__(cls, *args, **kwargs):
        raise RuntimeError('%s should not be instantiated' % cls)

class Settings(objectless):
    """Never to be instantiated."""
    _prompt = "davisql> "
    _version = "1.2"
    _copyright = "Â©2020 Chris Irwin Davis"
    _is_exit = False
    _page_size = 512

    @classmethod
    def is_exit(cls) -> bool:
        return cls._is_exit

    @classmethod
    def set_exit(cls, val: bool) -> None:
        cls._is_exit = val

    @classmethod
    def get_prompt(cls) -> str:
        return cls._prompt

    @classmethod
    def set_prompt(cls, val: str) -> None:
        cls._prompt = val

    @classmethod
    def get_version(cls) -> str:
        return cls._version

    @classmethod
    def set_version(cls, val: str) -> None:
        cls._version = val

    @classmethod
    def get_copyright(cls) -> str:
        return cls._copyright

    @classmethod
    def set_copyright(cls, val: str) -> None:
        cls._copyright = val

    @classmethod
    def get_page_size(cls) -> int:
        return cls._page_size

    @classmethod
    def set_page_size(cls, val: int) -> None:
        cls._page_size = val

    @staticmethod
    def line(rep_s: str, n_reps: int) -> str:
        return "".join([rep_s]*n_reps)


class SettingsTestSuite(unittest.TestCase):

    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName=methodName)
        
    def test_changes(self):
        self.assertEqual(Settings.get_copyright(), "None")
        self.assertEqual(Settings.get_page_size(), 1024)
        self.assertEqual(Settings.is_exit(), True)
        self.assertEqual(Settings.get_prompt(), "riodb> ")
        self.assertEqual(Settings.get_version(), "1.3")

if __name__ == "__main__":
    Settings.set_exit(True)
    Settings.set_prompt("riodb> ")
    Settings.set_version("1.3")
    Settings.set_page_size(1024)
    Settings.set_copyright("None")
    unittest.main()
