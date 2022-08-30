from typing import Optional

from hdx.api.configuration import Configuration


class TestResult:
    actual_result = None


testresult = TestResult()


def my_testfn():
    testresult.actual_result = Configuration.read().get_hdx_site_url()


def my_testkeyfn():
    testresult.actual_result = Configuration.read().get_api_key()


def my_testuafn():
    testresult.actual_result = Configuration.read().user_agent


def my_excfn():
    testresult.actual_result = Configuration.read().get_hdx_site_url()
    raise ValueError("Some failure!")


def my_testfnkw(**kwargs):
    fn = kwargs.get("fn")
    if fn == "site":
        testresult.actual_result = Configuration.read().get_hdx_site_url()
    elif fn == "api":
        testresult.actual_result = Configuration.read().get_api_key()
    elif fn == "agent":
        testresult.actual_result = Configuration.read().user_agent
    elif fn == "exc":
        testresult.actual_result = Configuration.read().get_hdx_site_url()
        raise ValueError("Some failure!")


def my_testfnia(mydata: Optional[str] = None, myflag: bool = False) -> str:
    """My test function. It takes in mydata an optional string which defaults to None.
    It assigns that to testresult which is an object of type TestResult. It returns
    mydata.

    Args:
        mydata (Optional[str]): Data. Defaults to None.
        myflag (bool): My flag. Defaults to False.

    Returns:
        str: String
    """
    testresult.actual_result = mydata
    return mydata
