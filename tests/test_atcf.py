import io

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



def test_radii_expansion():
    """Test that missing radii columns are created and filled with nulls"""
    testdeck = io.StringIO()
    testdeck.write("""
AL, 02, 2022062718,   , BEST,   0,  83N,  505W,  35, 1009, DB,  34, NEQ,  100,    0,    0,  120, 1012,  150, 120,  40,   0,   L,   0,    ,   0,   0,        TWO, S,  0,    ,    0,    0,    0,    0, genesis-num, 009, TRANSITIONED, alA42022 to al022022, DISSIPATED, al022022 to al942022, TRANSITIONED, alB42022 to al022022, 
AL, 02, 2022062800,   , BEST,   0,  87N,  522W,  35, 1009, DB,  34, NEQ,  100,    0,    0,  120, 1012,  150, 120,  45,   0,   L,   0,    ,   0,   0,        TWO, S, 12, NEQ,   60,    0,    0,    0, genesis-num, 009, 
AL, 02, 2022062806,   , BEST,   0,  91N,  542W,  35, 1009, DB,  34, NEQ,  100,    0,    0,  120, 1012,  150, 120,  45,   0,   L,   0,    ,   0,   0,        TWO, S, 12, NEQ,   60,    0,    0,    0, genesis-num, 009, 
""")
    testdeck.seek(0)
    adeck = tciopy.read_adeck(testdeck).collect()
    # check that rad50_NEQ column exists and is all null
    assert "rad50_NEQ" in adeck.columns
    assert adeck["rad50_NEQ"].is_null().all()
    assert "rad64_SEQ" in adeck.columns
    assert adeck["rad64_SEQ"].is_null().all()



if __name__ == "__main__":
    pytest.main()
    # print("All tests passed")
