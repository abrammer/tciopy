from pandas.testing import assert_frame_equal
import tciopy
import tciopy.atcf
import pathlib
import tempfile
import pytest


TEST_FILE = pathlib.Path(__file__).parent.parent / "data" / "aal032023.dat"

def test_circular_atcf(testfile=TEST_FILE):
    atcf = tciopy.read_adeck(testfile)
    # with tempfile.NamedTemporaryFile(mode="wt", delete_on_close=False) as outf:
    with open("test_output", "wt") as outf:
        tciopy.atcf.write_adeck(outf, atcf)
        outf.close()
        natcf = tciopy.read_adeck(outf.name)
    atcf = atcf.drop(columns="index")
    atcf = atcf.sort_values(by=["basin", "number", "datetime", "tech", "tau"])
    atcf.loc[:,['gusts','eye']] = atcf.loc[:,['gusts','eye']].fillna(0)
    natcf = natcf.drop(columns="index")
    natcf = natcf.sort_values(by=["basin", "number", "datetime", "tech", "tau"])
    natcf.loc[:,['gusts','eye']]  = natcf.loc[:,['gusts','eye']].fillna(0)
    assert_frame_equal(atcf, natcf, by_blocks=True)


if __name__ == "__main__":
    pytest.main()
    # print("All tests passed")
