import subprocess
import sys
import unittest
import os


root = os.path.join(os.getenv("HOME"), "Documents")
subprocess.call(
    ["which", "python3"],
    stdout=open(os.path.join(root, "python_executable.txt"), "wt"),
)

with open(os.path.join(root, "python_path.txt"), "wt") as fp:
    fp.write(str(sys.path))


class MyTest(unittest.TestCase):
    def test(self):
        print("Testing done!")


if __name__ == "__main__":
    pass
    unittest.main()
