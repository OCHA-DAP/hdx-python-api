from hdx.configuration import Configuration


class TestResult:
    actual_result = None


testresult = TestResult()


def my_testfn(configuration: Configuration):
    testresult.actual_result = configuration.get_hdx_site()


def my_excfn(configuration: Configuration):
    testresult.actual_result = configuration.get_hdx_site()
    raise ValueError('Some failure!')
