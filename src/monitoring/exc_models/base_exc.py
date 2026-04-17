class PipelineCrash(Exception):
    """Root Base Exceeption"""

    pass


class ProcessingFailed(PipelineCrash):
    """Base class to catch Failed of Extrak-Tranform-Format Data"""

    pass


class RoutingError(ProcessingFailed):
    """catch Routing Process Error"""

    pass


class FetchDataError(RoutingError):
    """catch Fetching process Error"""

    pass


class RateLimit(FetchDataError):
    pass


class AuthenticationError(FetchDataError):
    pass


class BLSRequestsError(FetchDataError):
    pass


class BEARequestsError(FetchDataError):
    pass


class FREDRequestsError(FetchDataError):
    pass


class ParseDataError(RoutingError):
    """catch Parser process Error"""

    pass


class BLSParserError(ParseDataError):
    pass


class BEAParserError(ParseDataError):
    pass


class FREDParserError(ParseDataError):
    pass


class StandardizedError(ProcessingFailed):
    """catch Error Standardizer data"""

    pass


class InvalidStandardized(StandardizedError):
    pass


class ResultsNotFound(ProcessingFailed):
    """catch Error Results Data"""

    pass


class FilterError(ProcessingFailed):
    """catch Error Filters data"""

    pass


class CalculateError(ProcessingFailed):
    """catch Error Calculation data"""

    pass


class FormatError(ProcessingFailed):
    """catch Error Format Final data"""

    pass


class UploadFailed(PipelineCrash):
    """catch Error Upload to DB"""

    pass


class ResourceNotFound(PipelineCrash):
    """catch Error Load Resource .env File"""

    pass
