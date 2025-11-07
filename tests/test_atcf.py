from polars.testing import assert_frame_equal
import tciopy
import tciopy.atcf
import pathlib
import tempfile
import pytest


TEST_FILE = pathlib.Path(__file__).parent / "small_deck.dat.gz"


def test_circular_atcf(testfile=TEST_FILE):
    atcf = tciopy.read_adeck(testfile)
    tempdir = tempfile.TemporaryDirectory()

    outfname = f"{tempdir.name}/test_output"
    with open(outfname, "wt") as outf:
        tciopy.atcf.write_adeck(outf, atcf)
    natcf = tciopy.read_adeck(outfname)
    assert_frame_equal(atcf.fill_nan(0), natcf.fill_nan(0))


if __name__ == "__main__":
    pytest.main()
    # print("All tests passed")
